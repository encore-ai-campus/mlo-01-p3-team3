"""One observation: registration, images, raw preservation, then event delivery."""
from ..contracts import digest, envelope, event_for, now, registration_identity
from ..errors import Conflict, LeaseLost
from ..files import confined
from ..retry import error_code
from ..ports import IngestionLedger, ImageStore, EventPublisher, AuditTrail


def check_registration(row, saved, settings):
    doc = saved["document"]
    # Lineage and first input snapshot are deliberately frozen on first registration.
    expected = envelope({**row, "lineage": doc["lineage"]}, doc["_id"], doc["input_snapshot_id"], settings.image_bucket)
    if (doc != expected or saved["target_fingerprint"] != settings.fingerprint
            or saved["observation_key"] != row["observation_key"] or saved["identity"] != registration_identity(row)):
        raise Conflict("REGISTRATION_DOCUMENT_CONFLICT")
    if saved["event"] != event_for(doc, saved["event"]["event_id"], saved["event"]["occurred_at"]):
        raise Conflict("REGISTRATION_EVENT_CONFLICT")



def process(row, *, snapshot_dir, settings, ledger: IngestionLedger, objects: ImageStore,
            publisher: EventPublisher, audit: AuditTrail,
            manifest, report, perform, point, notify, repair_raw=False, replay_events=False):
    key, token = row["observation_key"], None
    try:
        archived = audit.registration(key)
        if archived:
            check_registration(row, archived, settings)
        saved = perform(key, "REGISTER", lambda: ledger.register(row, manifest, archived))
        token = ledger.acquire(key)
        # register() may have returned state from before another worker completed.
        # Only fenced, freshly read evidence can authorize stage reuse.
        saved = ledger.get(key)
        ledger.renew(key, token)
        check_registration(row, saved, settings)
        if archived is None:
            # Freeze recovery identity only. Mongo's operational lease
            # contains native BSON datetimes and is not portable JSON.
            audit.write("registrations/" + key + ".json", {name: saved[name] for name in (
                'observation_key', 'target_fingerprint', 'identity', 'document', 'event')})
        elif archived["document"] != saved["document"] or archived["event"] != saved["event"]:
            raise Conflict("REGISTRATION_ARCHIVE_CONFLICT")
        point("REGISTERED")
        raw_confirmed = saved.get("raw_confirmed", saved.get("stage") in {
            "RAW_CONFIRMED", "EVENT_PENDING", "EVENT_UNKNOWN", "EVENT_CONFIRMED"})
        prior_vector = saved.get("image_state_vector", [])
        if saved.get("image_state_fingerprint") != digest(prior_vector):
            raise Conflict("IMAGE_STATE_FINGERPRINT_CONFLICT")
        prior = {item["image_slot"]: item for item in prior_vector}
        if len(prior) != len(prior_vector):
            raise Conflict("IMAGE_STATE_SLOT_CONFLICT")
        vector = []
        for image in row["images"]:
            slot = "representative" if image["role"] == "representative" else f"gallery:{image['order']}"
            state = {"image_slot": slot, "checksum": image.get("sha256"),
                     "bucket": settings.image_bucket if image.get("object_key") else None,
                     "object_key": image.get("object_key"), "availability": "UNCONFIRMED",
                     "verification": "sha256-bytes-v1"}
            previous = prior.get(slot)
            if previous:
                if any(previous.get(field) != value for field, value in state.items() if field != "availability"):
                    raise Conflict("IMAGE_STATE_IDENTITY_CONFLICT")
                state["availability"] = previous["availability"]
            vector.append(state)
        if set(prior) - {state["image_slot"] for state in vector}:
            raise Conflict("IMAGE_STATE_SLOT_CONFLICT")
        images_reused = bool(saved.get("image_state_checked_at")) and all(
            state["availability"] == "AVAILABLE" for state in vector)
        if not row["images"]:
            report["gallery_missing"] += 1
        for image, state in zip(row["images"], vector):
            slot = state["image_slot"]
            if state["availability"] == "AVAILABLE":
                report["image_relations_reused"] += 1
            elif image.get("object_key"):
                source = confined(snapshot_dir, image["object_key"])
                try:
                    written = perform(key, "IMAGE", lambda: objects.ensure(image["object_key"], source,
                        image["sha256"], image["bytes"]), token)
                    report["objects_written"] += bool(written)
                    state["availability"] = "AVAILABLE"
                except Conflict:
                    raise
                except LeaseLost:
                    raise
                except Exception as exc:
                    state["availability"] = "FAILED"
                    audit.write("image_errors/" + key + "-" + digest(slot) + ".json", {
                        "observation_key": key, "image_slot": slot, "error_code": error_code(exc)})
                # Each slot is durable before moving on to another external write.
                checkpoint = sorted(vector, key=lambda item: item["image_slot"])
                perform(key, "IMAGE_STATE", lambda: ledger.images(key, token, checkpoint), token)
                point("IMAGE_RESOLVED")
            report["image_relations_available" if state["availability"] == "AVAILABLE"
                   else "image_relations_unavailable"] += 1
        if images_reused:
            report["stages_reused"] += 1
        elif not any(image.get("object_key") for image in row["images"]):
            perform(key, "IMAGE_STATE", lambda: ledger.images(key, token, sorted(vector, key=lambda i: i["image_slot"])), token)
        if not raw_confirmed:
            ledger.update(key, token, stage="IMAGES_RESOLVED")
        point("IMAGES_RESOLVED")
        if raw_confirmed and not repair_raw:
            report["raw_reused"] += 1
            report["stages_reused"] += 1
        else:
            created = perform(key, "RAW", lambda: ledger.ensure_raw(saved["document"]), token)
            point("RAW_INSERTED_BEFORE_MARK")
            ledger.update(key, token, stage="RAW_CONFIRMED", raw_confirmed=True)
            report["raw_created"] += bool(created)
        report["raw_confirmed"] += 1
        point("RAW_CONFIRMED")
        if not saved.get("event_confirmed") or replay_events:
            ledger.update(key, token, stage="EVENT_PENDING")
            try:
                # Kafka performs bounded protocol retries itself, never multiply them here.
                perform(key, "PUBLISH", lambda: publisher.send(saved["event"], saved["document"]["source_record_key"]),
                        token, attempts=1)
                point("EVENT_SENT_BEFORE_MARK")
            except BaseException:
                ledger.update(key, token, stage="EVENT_UNKNOWN")
                raise
            ledger.update(key, token, stage="EVENT_CONFIRMED", event_confirmed=True, last_error=None)
            report["events_sent"] += 1
        else:
            ledger.update(key, token, stage="EVENT_CONFIRMED", last_error=None)
            report["events_reused"] += 1
            report["stages_reused"] += 1
        if images_reused and raw_confirmed and not repair_raw and saved.get("event_confirmed") and not replay_events:
            report["observations_reused"] += 1
        notify("observation_complete", key, raw_confirmed=report["raw_confirmed"], total=report["products"])
    except Exception as exc:
        code = error_code(exc)
        report["failed"] += 1
        report["errors"].append({"observation_key": key, "error_code": code})
        terminal = isinstance(exc, Conflict) or code in {"RAW_TOO_LARGE", "REGISTRATION_TOO_LARGE", "BSON_UNENCODABLE_INPUT"}
        audit.write(("quarantine/" if terminal else "failures/") + key + ".json", {
            "snapshot_id": manifest["snapshot_id"], "observation_key": key, "error_code": code,
            "classification": "TERMINAL_INPUT" if terminal else "RETRY_REQUIRED",
            "recoverable_input": "observations.jsonl", "recorded_at": now()})
        if token is not None:
            try:
                ledger.update(key, token, last_error=code)
            except Exception:
                pass  # neither successful state nor a false completion is written
        notify("observation_failed", key, error_code=code)
    finally:
        if token is not None:
            try:
                ledger.release(key, token)
            except Exception:
                report["errors"].append({"observation_key": key, "error_code": "LEASE_RELEASE_UNCONFIRMED"})
