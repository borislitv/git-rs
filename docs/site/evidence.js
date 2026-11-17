'use strict';

// Shared by the offline page and its allowlisted static builder.
(() => {
  const MAX_BYTES = 2 * 1024 * 1024;
  const numericFields = ['wall_seconds', 'user_cpu_seconds', 'system_cpu_seconds', 'peak_rss_bytes'];
  const stringFields = ['id', 'title', 'scope', 'revision', 'workload', 'outcome', 'limitations', 'source', 'flow_id', 'step_id'];
  const missing = (value) => value === undefined || value === null || value === '' || value === 'unknown';
  const record = (value) => value && typeof value === 'object' && !Array.isArray(value);
  const boundedText = (value) => typeof value === 'string' && value.length <= 16000;
  function validate(value) {
    if (!record(value) || value.format !== 'git-rs-architecture-evidence/v1' || !Array.isArray(value.items)) {
      throw new Error('Ожидается объект с format: git-rs-architecture-evidence/v1 и массивом items.');
    }
    if (value.items.length > 500) throw new Error('Допускается не более 500 записей.');
    return value.items.map((item, index) => {
      if (!record(item)) throw new Error(`Запись ${index + 1}: ожидается объект.`);
      const clean = {};
      stringFields.forEach((field) => {
        const entry = item[field];
        if (!missing(entry) && !boundedText(entry)) {
          throw new Error(`Запись ${index + 1}, ${field}: ожидается строка до 16000 символов или null.`);
        }
        clean[field] = missing(entry) ? null : entry;
      });
      numericFields.forEach((field) => {
        const entry = item[field];
        if (!missing(entry) && (typeof entry !== 'number' || !Number.isFinite(entry) || entry < 0)) {
          throw new Error(`Запись ${index + 1}, ${field}: ожидается конечное неотрицательное число или null/unknown.`);
        }
        clean[field] = missing(entry) ? null : entry;
      });
      if (item.takeaways !== undefined && (!Array.isArray(item.takeaways)
        || item.takeaways.length > 8 || item.takeaways.some((entry) => !boundedText(entry)))) {
        throw new Error(`Запись ${index + 1}, takeaways: ожидается до 8 поясняющих строк.`);
      }
      clean.takeaways = item.takeaways || [];
      if (item.hotspots !== undefined && (!Array.isArray(item.hotspots) || item.hotspots.length > 50)) {
        throw new Error(`Запись ${index + 1}, hotspots: ожидается массив до 50 элементов.`);
      }
      clean.hotspots = (item.hotspots || []).map((hotspot) => {
        if (!record(hotspot) || !boundedText(hotspot.name) || !boundedText(hotspot.detail)
          || typeof hotspot.self_percent !== 'number' || !Number.isFinite(hotspot.self_percent)
          || hotspot.self_percent < 0 || hotspot.self_percent > 100) {
          throw new Error(`Запись ${index + 1}: некорректный hotspot (строки и доля 0..100).`);
        }
        return { name: hotspot.name, self_percent: hotspot.self_percent, detail: hotspot.detail };
      });
      if (item.elapsed_spans !== undefined && (!Array.isArray(item.elapsed_spans) || item.elapsed_spans.length > 20)) {
        throw new Error(`Запись ${index + 1}, elapsed_spans: ожидается массив до 20 элементов.`);
      }
      clean.elapsed_spans = (item.elapsed_spans || []).map((span) => {
        if (!record(span) || !boundedText(span.name) || !boundedText(span.detail)
          || typeof span.seconds !== 'number' || !Number.isFinite(span.seconds) || span.seconds < 0) {
          throw new Error(`Запись ${index + 1}: некорректный elapsed span (строки и неотрицательные секунды).`);
        }
        return { name: span.name, seconds: span.seconds, detail: span.detail };
      });
      return clean;
    });
  }
  const api = Object.freeze({ validate, MAX_BYTES, numericFields });
  if (typeof module === 'object' && module.exports) module.exports = api;
  else globalThis.GitRsEvidence = api;
})();
