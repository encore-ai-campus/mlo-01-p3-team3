import {initAccountState} from "../sketch/account-state.mjs";
const $ = id => document.getElementById(id);
const base = document.body.dataset.baseUrl;
const {createProductPreview} = await import(base + "ui/product-preview.mjs");
const preview = createProductPreview({api, base, parent:()=>runId, panel:document.getElementById("product-preview")});
let epoch = 0;
let runId = new URLSearchParams(location.search).get("run");
let timer;
let csrf;
const statusNames = {PENDING:"추천을 준비하고 있습니다.", COMPLETED:"추천이 완료되었습니다.", UNVERIFIED_ONLY:"요청하신 조건을 확인할 수 있는 상품을 찾지 못했어요. 조건을 바꾸거나 입력을 수정해 다시 추천해 주세요.", NO_RESULTS:"조건에 맞는 상품이 없습니다.", AMBIGUOUS:"원하는 가구가 모호합니다. 입력 내용을 수정해 주세요.", UNSUPPORTED:"현재 지원하지 않는 요청입니다.", INDEX_NOT_READY:"상품 카탈로그가 준비되지 않았습니다.", RANKING_FAILED:"추천 계산에 실패했습니다."};
const fieldNames = {category_code:"품목", color:"색상", material:"재질", shape:"형태", price_krw:"예산", width_mm:"폭", depth_mm:"깊이", height_mm:"높이", unapplied_conditions:"추가 조건"};
const checkNames = {MATCH:"확인", DIFFERENT:"불일치", UNKNOWN:"미확인"};
function node(tag, text) { const el = document.createElement(tag); el.textContent = text; return el; }
const valueNames = {black:"검정", white:"흰색", gray:"회색", brown:"갈색", beige:"베이지", ivory:"아이보리", navy:"네이비", blue:"파랑", pink:"분홍", green:"초록", red:"빨강", yellow:"노랑", wood:"목재", metal:"금속", glass:"유리", ceramic:"세라믹", fabric:"패브릭", mesh:"메쉬", synthetic_leather:"인조가죽", leather:"천연가죽", plastic:"플라스틱", mdf:"MDF", rectangle:"사각형", square:"정사각형", round:"원형", oval:"타원형", l_shape:"ㄱ자형", straight:"일자형"};
const componentNames = {seat_width_mm:"좌판 폭", seat_depth_mm:"좌판 깊이", seat_height_mm:"좌판 높이", back_width_mm:"등판 폭", back_depth_mm:"등판 깊이", back_height_mm:"등판 높이", mattress_width_mm:"매트리스 폭", mattress_depth_mm:"매트리스 깊이", mattress_height_mm:"매트리스 높이", top_material:"상판 재질", frame_material:"프레임 재질", seat_material:"좌판 재질", back_material:"등판 재질", frame_shape:"프레임 형태"};
function intervalText(interval) { return interval.minimum === interval.maximum ? String(interval.minimum) : `${interval.minimum}~${interval.maximum}`; }
function dimensionText(metadata, field) { return metadata[field] ?? (metadata.dimension_ranges_mm?.[field] ? intervalText(metadata.dimension_ranges_mm[field]) : "미확인"); }
async function api(path, options={}) {
  const started = epoch;
  const response = await fetch(base + path, {...options, headers:{...(options.headers||{}), "X-CSRFToken":csrf}, credentials:"same-origin", cache:"no-store"});
  const value = await response.json().catch(() => ({error:"요청을 처리하지 못했습니다. 다시 시도해 주세요."}));
  if (started !== epoch) throw new Error("계정이 변경되었습니다. 현재 계정으로 다시 요청해 주세요.");
  if (!response.ok) { const error = new Error(value.error || "요청을 처리하지 못했습니다."); error.status = response.status; throw error; }
  return value;
}
function cards(target, items, emptyMessage = "표시할 상품이 없습니다.") {
  target.replaceChildren();
  for (const item of items) {
    const card = node("article", ""); card.className="candidate";
    const image = node("img", ""); image.alt=item.name; image.loading="lazy"; image.src=`${base}assets/${encodeURIComponent(runId)}/${item.image}`;
    const title = node("strong", item.name);
    if (item.metadata.comparison_url) {
      const href = `${base}api/runs/${encodeURIComponent(runId)}/product-link?image_id=${encodeURIComponent(item.image_id)}`;
      const link = text => { const a=node("a", text); a.href=href; a.target="_blank"; a.rel="noopener noreferrer"; return a; };
      const heading=link(""); heading.append(image, title); card.append(heading, link("다나와에서 보기"));
    } else {
      card.append(image, title, node("p", "상품 링크 미확인"));
    }
    const m = item.metadata;
    const missing = [...new Set(Object.entries(item.checks).filter(([,value])=>value === "UNKNOWN")
      .map(([key])=>fieldNames[key.split(":").at(-1)] || "추가 조건"))];
    if (missing.length) card.append(node("p", `추가 확인 필요: ${missing.join(", ")}`));
    for (const value of Object.values(item.preference_checks || {})) {
      card.append(node("p", `재질 선호 대조 (검색 참고): ${checkNames[value] || "미확인"}`));
    }
    card.append(node("p", `가격: ${m.price_krw == null ? "미확인" : m.price_krw.toLocaleString()+"원"}`));
    if (m.observed_at) card.append(node("p", `상품 정보 수집: ${m.observed_at}`));
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
    const checks = Object.entries(item.checks).map(([key,value])=>`${fieldNames[key.split(":").at(-1)] || "요청 조건"}: ${checkNames[value] || "미확인"}`).join(" · ");
    for (const reason of item.match_explanation || []) card.append(node("p", reason));
    const roomLabels = {palette:"색조", materials:"재질", shapes:"형태", styles:"스타일"};
    const unknownRoom = Object.entries(item.interior_match?.checks || {}).filter(([,v])=>v.state === "UNKNOWN").map(([key])=>roomLabels[key]);
    if (unknownRoom.length) card.append(node("p", `상품 정보가 부족해 방과의 조화를 비교하지 못한 특징: ${unknownRoom.join(", ")}`));
    card.append(node("p", checks), node("p", "실제 공간의 크기 적합성: 미확인"));
    const generate=node("button", "이 상품을 방에 배치해 보기"); generate.type="button";
    generate.addEventListener("click", () => preview.start(item)); card.append(generate);
    target.append(card);
  }
  if (!items.length && emptyMessage) target.append(node("p", emptyMessage));
}
async function poll() {
  clearTimeout(timer);
  try {
    const state = await api(`api/runs/${encodeURIComponent(runId)}`);
    const jobStatus = state.job?.status;
    const running = ["STAGING", "QUEUED", "RUNNING", "RETRY_WAIT"].includes(jobStatus);
    const progress = jobStatus === "QUEUED" || jobStatus === "STAGING" ? "추천 순서를 기다리고 있습니다." : jobStatus === "RETRY_WAIT" ? "잠시 후 자동으로 다시 시도합니다." : "입력을 해석하고 상품을 찾고 있습니다.";
    $("status").textContent = jobStatus === "FAILED" ? state.job.error : running ? progress : statusNames[state.status] || state.status;
    $("retry").hidden = state.job?.status !== "FAILED" || state.retry_supported === false;
    if (state.room_image) { $("room").src=`${base}assets/${encodeURIComponent(runId)}/${state.room_image}`; $("room").hidden=false; }
    if (state.sketch_image) { $("sketch-overlay").src=`${base}assets/${encodeURIComponent(runId)}/${state.sketch_image}`; $("sketch-overlay").hidden=false; }
    $("requested-text").textContent=state.text || "별도 설명 없이 스케치로 요청했어요.";
    const preferences = state.external_project?.style_preferences || [];
    $("style-preferences").hidden=!preferences.length;
    const colorStyle = state.external_project?.style_policy === "color-first-v2";
    $("style-preferences").textContent=preferences.length ? `선호 스타일 (${colorStyle ? "색상·톤 참고" : "검색 참고"}): ${preferences.join(", ")}` : "";
    $("legacy-preference-notice").hidden=!(state.external_project?.id && !state.external_project?.style_policy && state.text.includes("\n선호 스타일:"));
    const r = state.recommendations;
    if (colorStyle && preferences.length && r?.color_preference) {
      const preference = r.color_preference;
      const color = valueNames[preference.value] || preference.value;
      $("style-preferences").textContent += preference.source === "style" ? ` · ${color} 계열을 우선 반영했어요.` : preference.source === "context" ? ` · 요청사항의 ${color} 선호를 우선 반영했어요.` : "";
    }
    const softMaterial = r?.ranking_version === "weighted-content-v2";
    $("items-heading").textContent=softMaterial ? "추천 상품" : "조건을 확인한 상품";
    $("material-policy-notice").hidden=!softMaterial;
    const emptyItemsMessage = running ? "상품을 찾고 있어요. 잠시만 기다려 주세요." : "";
    cards($("items"), r?.items || [], emptyItemsMessage);
    window.dispatchEvent(new Event("sketchon:recommendations-ready"));
    $("verified-panel").hidden = !r?.items?.length && !running;
    if (running) timer=setTimeout(poll, jobStatus === "RETRY_WAIT" ? 3000 : 1200);
  } catch (error) {
    $("status").textContent=error.message;
    const denied = [403,404].includes(error.status);
    $("retry").hidden=denied || !runId;
    if (denied) clearResults();
  }
}
function clearResults() {
  preview.clear();
  $("items").replaceChildren(); $("verified-panel").hidden=true;
  $("room").removeAttribute("src"); $("room").hidden=true;
  $("sketch-overlay").removeAttribute("src"); $("sketch-overlay").hidden=true;
  $("requested-text").textContent="입력 내용을 표시할 수 없습니다.";
  $("style-preferences").textContent=""; $("style-preferences").hidden=true;
  $("legacy-preference-notice").hidden=true; $("verified-panel").hidden=false;
  $("material-policy-notice").hidden=true; $("items-heading").textContent="추천 상품";
}
$("retry").addEventListener("click", async () => {
  try { await api(`api/runs/${encodeURIComponent(runId)}/actions/prepare`, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify({retry:true})}); await poll(); }
  catch (error) { $("status").textContent=error.message; }
});
api("api/csrf").then(value=>{csrf=value.csrf_token; if(runId) poll();}).catch(error=>{$("status").textContent=error.message;});
initAccountState({onChange: (state, {changed}) => {
  if (state.csrf_token) csrf=state.csrf_token;
  if (changed) {
    epoch++; clearTimeout(timer); runId=null; clearResults();
    $("retry").hidden=true; history.replaceState(null,"",base);
    $("status").textContent="계정이 변경되었습니다. 현재 계정으로 다시 요청해 주세요.";
  }
}});
window.addEventListener("pagehide", () => { clearTimeout(timer); });
window.addEventListener("pageshow", event => { if (event.persisted && runId) void poll(); });
