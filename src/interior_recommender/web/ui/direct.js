"use strict";
const $ = id => document.getElementById(id);
let runId = new URLSearchParams(location.search).get("run");
let timer;
let csrf;
const statusNames = {PENDING:"추천을 준비하고 있습니다.", COMPLETED:"추천이 완료되었습니다.", UNVERIFIED_ONLY:"필수 정보가 확인된 상품이 없습니다. 미확인 후보를 따로 표시합니다.", NO_RESULTS:"조건에 맞는 상품이 없습니다.", AMBIGUOUS:"원하는 가구가 모호합니다. 입력 내용을 수정해 주세요.", UNSUPPORTED:"현재 지원하지 않는 요청입니다.", INDEX_NOT_READY:"상품 카탈로그가 준비되지 않았습니다.", RANKING_FAILED:"추천 계산에 실패했습니다."};
const fieldNames = {category_code:"품목", color:"색상", material:"재질", shape:"형태", price_krw:"예산", width_mm:"폭", depth_mm:"깊이", height_mm:"높이", unapplied_conditions:"추가 조건"};
const checkNames = {MATCH:"확인", DIFFERENT:"불일치", UNKNOWN:"미확인"};
const basisNames = {NONE:"기준 치수 없음", RELATIVE_ONLY:"사진 속 상대 비율", TYPICAL_OBJECT_SIZE:"주변 물체의 통상 크기를 가정"};
function node(tag, text) { const el = document.createElement(tag); el.textContent = text; return el; }
async function api(path, options={}) {
  const response = await fetch(path, {...options, headers:{...(options.headers||{}), "X-CSRFToken":csrf}, credentials:"same-origin"});
  const value = await response.json();
  if (!response.ok) throw new Error(value.error || "요청을 처리하지 못했습니다.");
  return value;
}
function cards(target, items) {
  target.replaceChildren();
  for (const item of items) {
    const card = node("article", ""); card.className="candidate";
    const image = node("img", ""); image.alt=item.name; image.src=`assets/${encodeURIComponent(runId)}/${item.image}`;
    card.append(image, node("strong", item.name));
    const m = item.metadata;
    card.append(node("p", `가격: ${m.price_krw == null ? "미확인" : m.price_krw.toLocaleString()+"원"}`));
    card.append(node("p", `규격 (폭/깊이/높이 mm): ${[m.width_mm,m.depth_mm,m.height_mm].map(x=>x??"미확인").join(" / ")}`));
    const checks = Object.entries(item.checks).map(([key,value])=>`${fieldNames[key.split(":").at(-1)] || "요청 조건"}: ${checkNames[value] || "미확인"}`).join(" · ");
    card.append(node("p", checks), node("p", "실제 공간의 크기 적합성: 미확인"));
    target.append(card);
  }
  if (!items.length) target.append(node("p", "표시할 상품이 없습니다."));
}
async function poll() {
  clearTimeout(timer);
  try {
    const state = await api(`api/runs/${encodeURIComponent(runId)}`);
    const running = state.job?.status === "RUNNING";
    $("status").textContent = state.job?.status === "FAILED" ? state.job.error : running ? "입력을 해석하고 상품을 찾고 있습니다." : statusNames[state.status] || state.status;
    $("retry").hidden = state.job?.status !== "FAILED";
    if (state.room_image) { $("room").src=`assets/${encodeURIComponent(runId)}/${state.room_image}`; $("room").hidden=false; }
    const c = state.context;
    if (c) {
      $("context").hidden=false;
      $("context").replaceChildren(node("h2", "입력 해석"), node("p", c.reason), node("p", c.room_context), node("p", `배치: ${c.placement_intent || "미확인"}`), node("p", `크기 근거: ${basisNames[c.size_estimate.basis] || "미확인"} · ${c.size_estimate.explanation}`));
      if (c.unapplied_conditions.length) $("context").append(node("p", `별도 확인이 필요한 조건: ${c.unapplied_conditions.join(", ")}`));
    }
    const r = state.recommendations;
    cards($("items"), r?.items || []); cards($("unverified"), r?.unverified_items || []);
    $("unverified-panel").hidden = !r?.unverified_items?.length;
    if (running) timer=setTimeout(poll, 1200);
  } catch (error) { $("status").textContent=error.message; $("retry").hidden=false; }
}
$("request-form").addEventListener("submit", async event => {
  event.preventDefault(); clearTimeout(timer);
  const button=event.currentTarget.querySelector("button"); button.disabled=true;
  try {
    const form=new FormData(event.currentTarget); form.set("request_id", crypto.randomUUID());
    const result=await api("api/runs", {method:"POST", body:form}); runId=result.run_id;
    history.replaceState(null, "", `?run=${encodeURIComponent(runId)}`); await poll();
  } catch (error) { $("status").textContent=error.message; }
  finally { button.disabled=false; }
});
$("retry").addEventListener("click", async () => {
  try { await api(`api/runs/${encodeURIComponent(runId)}/actions/prepare`, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({retry:true})}); await poll(); }
  catch (error) { $("status").textContent=error.message; }
});
api("api/csrf").then(value=>{csrf=value.csrf_token; $("request-form").querySelector("button").disabled=false; if(runId) poll();}).catch(error=>{$("status").textContent=error.message;});
