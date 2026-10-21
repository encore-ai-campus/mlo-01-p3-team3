// Shared by the standalone screen and the mounted SketchOn screen.
export function createProductPreview({api, base = "", parent, panel}) {
  const status = panel.querySelector("[data-preview-status]");
  const image = panel.querySelector("[data-preview-image]");
  const title = panel.querySelector("[data-preview-title]");
  const retry = panel.querySelector("[data-preview-retry]");
  let token = 0, timer, child = null;
  function clear() {
    token++; clearTimeout(timer); child = null;
    panel.hidden = true; retry.hidden = true;
    image.hidden = true; image.removeAttribute("src");
  }
  async function poll(current) {
    try {
      const state = await api(`api/runs/${encodeURIComponent(child)}`);
      if (current !== token) return;
      const running = ["STAGING", "QUEUED", "RUNNING", "RETRY_WAIT"].includes(state.job?.status);
      retry.hidden = state.job?.status !== "FAILED";
      title.textContent = `${state.selected_product.name} · 배치 미리보기`;
      status.textContent = state.job?.status === "FAILED" ? state.job.error : running ? "선택한 상품을 방에 배치하고 있어요. 잠시만 기다려 주세요." :
        `${state.demo ? "데모 이미지 · 실제 생성 API를 호출하지 않았습니다. " : ""}${state.disclaimer}${state.unverified_candidate ? " 선택 상품은 요청 조건 충족 여부가 미확인된 후보입니다." : ""}`;
      if (state.result_image && state.job?.status === "SUCCEEDED") {
        image.src = `${base}assets/${encodeURIComponent(child)}/${state.result_image}`;
        image.hidden = false;
      }
      if (running) timer = setTimeout(() => poll(current), 1500);
    } catch (error) {
      if (current !== token) return;
      status.textContent = error.message;
    }
  }
  async function start(item) {
    clear(); const current = token;
    panel.hidden = false;
    title.textContent = `${item.name} · 배치 미리보기`;
    status.textContent = "미리보기 작업을 접수하고 있어요.";
    try {
      const result = await api(`api/runs/${encodeURIComponent(parent())}/previews`, {
        method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({image_id: item.image_id})
      });
      if (current !== token) return;
      child = result.run_id;
      await poll(current);
    } catch (error) {
      if (current === token) status.textContent = error.message;
    }
  }
  retry.addEventListener("click", async () => {
    const current = token;
    retry.disabled = true;
    try {
      await api(`api/runs/${encodeURIComponent(child)}/actions/prepare`, {
        method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({retry: true})
      });
      if (current === token) await poll(current);
    } catch (error) { if (current === token) status.textContent = error.message; }
    finally { retry.disabled = false; }
  });
  window.addEventListener("pagehide", () => { token++; clearTimeout(timer); });
  window.addEventListener("pageshow", event => { if (event.persisted && child) void poll(token); });
  return {start, clear};
}
