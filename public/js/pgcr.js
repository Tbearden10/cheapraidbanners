const pgcrCache = {};

window.showPGCRModal = async function(instanceId) {
  if (!instanceId) return;
  const modal = document.createElement('div');
  modal.className = 'pgcr-modal';
  modal.innerHTML = `
    <div class="pgcr-backdrop" onclick="this.parentElement.remove()"></div>
    <div class="pgcr-dialog">
      <button class="pgcr-close" onclick="this.closest('.pgcr-modal').remove()">×</button>
      <div class="pgcr-content"></div>
    </div>
  `;
  document.body.appendChild(modal);
  const contentEl = modal.querySelector('.pgcr-content');
  contentEl.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--chocolate);">Loading activity details...</div>';
  if (pgcrCache[instanceId]) {
    contentEl.innerHTML = JSON.stringify(pgcrCache[instanceId]);
    return;
  }
  try {
    const res = await fetch(`/pgcr?instanceId=${encodeURIComponent(instanceId)}`);
    const data = await res.json();
    if (!data || data.error) {
      contentEl.innerHTML = `<div style="text-align: center; padding: 40px; color: #dc143c;">Failed to load activity: ${data?.error || 'Unknown error'}</div>`;
      return;
    }
    pgcrCache[instanceId] = data;
    contentEl.innerHTML = JSON.stringify(data); // Replace with your actual render logic if needed
  } catch {
    contentEl.innerHTML = '<div style="text-align: center; padding: 40px; color: #dc143c;">Failed to load activity details</div>';
  }
};