"""Integration example ONLY; no routes are installed and no network call runs on import.

Future Application backend usage:
1. Authenticate/authorize the project and recommendation context in YOUR backend.
2. Load visitor_id, request_id, context_id and recommendation state on the server.
   Keep the request_id for retries; do not mint new IDs to bypass NO_FILL/expiry.
3. Call display_for_owned_context and return its safe projection to the browser.
4. Bind returned decision_id to that authorized context on the Application server.
5. For events, check that binding first, then call event_for_owned_decision.
   Forward only a navigation with allowed=True; never accept a destination from UI.

Browser lifecycle: RENDERED after image load; LOAD_ERROR on failure; VISIBLE only
after >=50% visibility continuously for 1000ms while the tab is active; CLICK on
actual activation. Keep event_id unchanged on retry. Hide on expiry/UNAVAILABLE/
NO_FILL and keep ordinary recommendations working. Do not automatically re-auction.
These entry points are server-side helpers, NOT authentication implementations.
"""
from advertising.integration import AdServerClient, recommendation_context


def build_client(internal_origin, public_origin, service_token, timeout_seconds):
    # Internal origin may be a VM hostname; public origin must be browser-reachable.
    # Token stays in backend environment/secret storage, never HTML/JavaScript.
    return AdServerClient(internal_origin, service_token,
                          public_base_url=public_origin, timeout_seconds=timeout_seconds)


def display_for_owned_context(client, state, *, request_id, visitor_id,
                              context_id, slot_id, confirmed_signals):
    body=recommendation_context(state, request_id=request_id, visitor_id=visitor_id,
                                context_id=context_id, placement=slot_id,
                                confirmed_signals=confirmed_signals)
    return client.display(body)


def event_for_owned_decision(client, decision_id, *, visitor_id, event):
    # visitor_id comes from the authorized server context, never the posted body.
    allowed={'event_id','kind','occurred_at','rendered','fraction','continuous_ms','active'}
    if not isinstance(event,dict) or set(event)-allowed:
        raise ValueError('Unexpected event fields')
    return client.event(decision_id,{**event,'visitor_id':visitor_id})
