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
const valueNames = {black:"검정", white:"흰색", gray:"회색", brown:"갈색", beige:"베이지", ivory:"아이보리", navy:"네이비", blue:"파랑", pink:"분홍", green:"초록", red:"빨강", yellow:"노랑", wood:"목재", metal:"금속", glass:"유리", ceramic:"세라믹", fabric:"패브릭", mesh:"메쉬", synthetic_leather:"인조가죽", leather:"천연가죽", plastic:"플라스틱", mdf:"MDF", rectangle:"사각형", square:"정사각형", round:"원형", oval:"타원형", l_shape:"ㄱ자형", straight:"일자형"};
const componentNames = {seat_width_mm:"좌판 폭", seat_depth_mm:"좌판 깊이", seat_height_mm:"좌판 높이", back_width_mm:"등판 폭", back_depth_mm:"등판 깊이", back_height_mm:"등판 높이", mattress_width_mm:"매트리스 폭", mattress_depth_mm:"매트리스 깊이", mattress_height_mm:"매트리스 높이", top_material:"상판 재질", frame_material:"프레임 재질", seat_material:"좌판 재질", back_material:"등판 재질", frame_shape:"프레임 형태"};
function intervalText(interval) { return interval.minimum === interval.maximum ? String(interval.minimum) : `${interval.minimum}~${interval.maximum}`; }
function dimensionText(metadata, field) { return metadata[field] ?? (metadata.dimension_ranges_mm?.[field] ? intervalText(metadata.dimension_ranges_mm[field]) : "미확인"); }
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
    const title = node("strong", item.name);
    if (item.metadata.comparison_url) {
      const href = `api/runs/${encodeURIComponent(runId)}/product-link?image_id=${encodeURIComponent(item.image_id)}`;
      const link = text => { const a=node("a", text); a.href=href; a.target="_blank"; a.rel="noopener noreferrer"; return a; };
      const heading=link(""); heading.append(image, title); card.append(heading, link("다나와에서 보기"));
    } else {
      card.append(image, title, node("p", "상품 링크 미확인"));
    }
    const m = item.metadata;
    card.append(node("p", `가격: ${m.price_krw == null ? "미확인" : m.price_krw.toLocaleString()+"원"}`));
    card.append(node("p", `전체 규격 (폭/깊이/높이 mm): ${["width_mm","depth_mm","height_mm"].map(field=>dimensionText(m,field)).join(" / ")}`));
    for (const [key, interval] of Object.entries(m.component_dimensions_mm || {})) {
      if (componentNames[key]) card.append(node("p", `${componentNames[key]}: ${intervalText(interval)} mm`));
    }
    for (const field of ["color", "material", "shape"]) {
      const options=m.attribute_options?.[field] || [];
      const values=m[field] != null ? [m[field]] : options;
      card.append(node("p", `${fieldNames[field]}${m[field] == null && options.length ? " 옵션 (미확정)" : ""}: ${values.length ? values.map(v=>valueNames[v] || v).join(" / ") : "미확인"}`));
    }
    for (const [key, values] of Object.entries(m.component_attributes || {})) {
      if (componentNames[key]) card.append(node("p", `${componentNames[key]}: ${values.map(v=>valueNames[v] || v).join(" / ")}`));
    }
    if (m.metadata_issues?.length) card.append(node("p", "일부 원천 규격은 확인이 필요합니다."));
    if (Object.keys(m.raw_attributes || {}).length) {
      const details=node("details", ""); details.append(node("summary", "수집 규격 원문"));
      for (const [key, values] of Object.entries(m.raw_attributes)) details.append(node("p", `${key}: ${values.join(" / ")}`));
      card.append(details);
    }
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
