'use strict';

(() => {
  const raw = globalThis.GIT_RS_ARCH;
  const data = raw && typeof raw === 'object' ? raw : {};
  const list = (value) => Array.isArray(value) ? value.filter((item) => item && typeof item === 'object') : [];
  const text = (value, fallback = 'Не указано') => {
    if (typeof value === 'string' && value.trim()) return value;
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    if (Array.isArray(value)) return value.map((item) => text(item, '')).filter(Boolean).join('\n') || fallback;
    return fallback;
  };
  const components = list(data.components);
  const flows = list(data.flows);
  const byId = (id) => document.getElementById(id);
  const el = (tag, className, content) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (content !== undefined) node.textContent = text(content, '');
    return node;
  };
  const empty = (target, title, message) => {
    const box = el('div', 'empty-state');
    box.append(el('strong', '', title), el('p', '', message));
    target.replaceChildren(box);
  };
  const badge = (value, style = '') => el('span', `badge ${style}`, value);
  const facts = (pairs) => {
    const dl = el('dl', 'fact-list');
    pairs.forEach(([label, value]) => {
      const row = el('div');
      row.append(el('dt', '', label), el('dd', '', text(value, 'Нет описания в архитектурных данных')));
      dl.append(row);
    });
    return dl;
  };

  // Only public repository paths and an explicit revision become navigable links.
  const sources = (value) => {
    const box = el('div', 'sources');
    const entries = Array.isArray(value) ? value : value ? [value] : [];
    entries.forEach((entry) => {
      let source = entry;
      if (typeof entry === 'string') {
        const match = /^(.*?)(?:#L(\d+))?$/.exec(entry);
        if (!match) {
          box.append(el('span', 'muted', entry));
          return;
        }
        source = { path: match[1], line: match[2] };
      }
      if (!source || typeof source !== 'object') return;
      const path = text(source.path, '');
      const revision = text(data.revision, '');
      const parts = path.split('/');
      const safePath = path && !path.startsWith('/') && !/[\\\u0000-\u001f:?#]/.test(path)
        && parts.every((part) => part && part !== '.' && part !== '..');
      const safeRevision = revision && /^[a-zA-Z0-9._/-]+$/.test(revision)
        && revision.split('/').every((part) => part && part !== '.' && part !== '..');
      if (!safePath || !safeRevision) {
        box.append(el('span', 'muted', text(source.label, path || 'Источник без пути')));
        return;
      }
      const link = el('a', 'source-link', text(source.label, path));
      const line = Number(source.line);
      const anchor = Number.isSafeInteger(line) && line > 0 ? `#L${line}` : '';
      link.href = `https://github.com/borislitv/git-rs/blob/${revision.split('/').map(encodeURIComponent).join('/')}/${parts.map(encodeURIComponent).join('/')}${anchor}`;
      link.target = '_blank';
      link.rel = 'noopener noreferrer';
      link.referrerPolicy = 'no-referrer';
      link.append(document.createTextNode(`${anchor ? ` · L${line}` : ''} ↗`));
      box.append(link);
    });
    return box;
  };

  byId('page-title').textContent = text(data.title, 'Внутри git-rs');
  document.title = `${text(data.title, 'git-rs')} · Атлас архитектуры`;
  byId('intro').textContent = text(data.intro, 'Архитектурные данные пока не подключены. Навигация и локальный импорт evidence доступны.');
  byId('revision').textContent = text(data.revision, 'не указана');

  const tabs = [...document.querySelectorAll('[role="tab"]')];
  function activateTab(name, focus = false) {
    const selected = tabs.find((tab) => tab.dataset.tab === name) || tabs[0];
    tabs.forEach((tab) => {
      const active = tab === selected;
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
      byId(tab.getAttribute('aria-controls')).hidden = !active;
    });
    if (focus) selected.focus();
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => { window.location.hash = tab.dataset.tab; activateTab(tab.dataset.tab); });
    tab.addEventListener('keydown', (event) => {
      let next;
      if (event.key === 'ArrowRight') next = (index + 1) % tabs.length;
      if (event.key === 'ArrowLeft') next = (index + tabs.length - 1) % tabs.length;
      if (event.key === 'Home') next = 0;
      if (event.key === 'End') next = tabs.length - 1;
      if (next === undefined) return;
      event.preventDefault();
      window.location.hash = tabs[next].dataset.tab;
      activateTab(tabs[next].dataset.tab, true);
    });
  });
  window.addEventListener('hashchange', () => {
    const name = window.location.hash.slice(1);
    if (name !== 'main') activateTab(name);
  });
  activateTab(window.location.hash.slice(1));

  let selectedFlow = 0;
  let selectedStep = 0;
  let evidenceItems = [];
  const componentButtons = [];
  function showComponent(index) {
    const component = components[index];
    if (!component) return;
    componentButtons.forEach((button, i) => button.setAttribute('aria-pressed', String(i === index)));
    const panel = byId('component-detail');
    panel.replaceChildren(el('span', 'eyebrow', 'КОМПОНЕНТ СИСТЕМЫ'), el('h3', '', component.title), el('p', '', component.summary));
    panel.append(sources(component.source));
    panel.append(el('h4', '', 'Где участвует'));
    const related = el('ul', 'related-list');
    flows.forEach((flow, flowIndex) => {
      list(flow.steps).forEach((step, stepIndex) => {
        if (typeof component.id !== 'string' || step.component !== component.id) return;
        const item = el('li');
        const button = el('button', 'related-button');
        button.type = 'button';
        button.append(el('span', '', `${text(flow.title)} · ${text(step.title)}`), el('span', '', '↗'));
        button.addEventListener('click', () => {
          selectedFlow = flowIndex;
          selectedStep = stepIndex;
          renderFlow();
          window.location.hash = 'flows';
          activateTab('flows');
          byId('step-detail').focus();
        });
        item.append(button);
        related.append(item);
      });
    });
    if (related.children.length) panel.append(related);
    else panel.append(el('p', 'muted', 'Связанные шаги в данных не указаны.'));
  }
  byId('component-count').textContent = String(components.length);
  components.forEach((component, index) => {
    const button = el('button', 'component');
    button.type = 'button';
    button.setAttribute('aria-pressed', 'false');
    button.setAttribute('aria-controls', 'component-detail');
    button.append(el('span', 'component-index', String(index + 1).padStart(2, '0')), el('strong', '', component.title), el('span', 'component-summary', component.summary));
    button.addEventListener('click', () => showComponent(index));
    componentButtons.push(button);
    byId('component-map').append(button);
  });
  if (components.length) showComponent(0);
  else {
    empty(byId('component-map'), 'Карта ещё не заполнена', 'Добавьте components в architecture-data.js.');
    empty(byId('component-detail'), 'Выберите компонент', 'После подключения данных здесь появятся описание, исходники и связанные запросы.');
  }

  const picker = byId('flow-select');
  flows.forEach((flow, index) => {
    const option = el('option', '', flow.title);
    option.value = String(index);
    picker.append(option);
  });
  picker.disabled = !flows.length;
  if (!flows.length) picker.append(el('option', '', 'Нет описанных запросов'));
  picker.addEventListener('change', () => {
    selectedFlow = Number(picker.value);
    selectedStep = 0;
    renderFlow();
  });
  byId('step-detail').tabIndex = -1;
  function renderStep() {
    const steps = list(flows[selectedFlow]?.steps);
    const step = steps[selectedStep];
    const panel = byId('step-detail');
    if (!step) {
      empty(panel, 'Шаги не описаны', 'Подробности появятся после подключения steps выбранного запроса.');
      return;
    }
    [...byId('step-diagram').querySelectorAll('button')].forEach((button, index) => button.setAttribute('aria-pressed', String(index === selectedStep)));
    panel.replaceChildren(el('span', 'eyebrow', `ШАГ ${String(selectedStep + 1).padStart(2, '0')} / ${String(steps.length).padStart(2, '0')}`), el('h3', '', step.title), el('p', '', step.summary));
    const resources = el('dl', 'detail-grid');
    [['CPU', step.cpu], ['RAM', step.memory], ['ВРЕМЯ', step.time]].forEach(([label, value]) => {
      const cell = el('div', 'resource-cell');
      cell.append(el('dt', '', label), el('dd', '', text(value, 'Нет оценки в данных')));
      resources.append(cell);
    });
    const component = components.find((item) => item.id === step.component);
    panel.append(resources, facts([
      ['Компонент', component ? component.title : step.component],
      ['Что происходит', step.details], ['I/O и ожидания', step.io],
      ['Проверки и инварианты', step.checks], ['Копии и владение данными', step.copies],
      ['Лимиты и admission', step.limits], ['Что наблюдать', step.metrics],
    ]), sources(step.sources));
    const flow = flows[selectedFlow];
    const matched = evidenceItems.filter((item) => typeof flow.id === 'string' && flow.id
      && typeof step.id === 'string' && step.id
      && item.flow_id === flow.id && item.step_id === step.id);
    if (matched.length) {
      panel.append(el('h4', '', 'Локальные evidence этого шага'));
      matched.forEach((item) => panel.append(evidenceCard(item)));
    }
  }
  function renderFlow() {
    const flow = flows[selectedFlow];
    const diagram = byId('step-diagram');
    diagram.replaceChildren();
    if (!flow) {
      empty(byId('flow-heading'), 'Пути запросов ещё не подключены', 'Ожидается массив flows в architecture-data.js.');
      renderStep();
      return;
    }
    picker.value = String(selectedFlow);
    byId('flow-heading').replaceChildren(el('h3', '', flow.title), el('p', '', flow.summary), badge(`Вход: ${text(flow.entry)}`));
    list(flow.steps).forEach((step, index) => {
      const item = el('li');
      const button = el('button', 'step-button');
      button.type = 'button';
      button.setAttribute('aria-controls', 'step-detail');
      button.setAttribute('aria-pressed', String(index === selectedStep));
      button.setAttribute('aria-label', `Шаг ${index + 1}: ${text(step.title)}`);
      const label = el('span');
      const component = components.find((entry) => entry.id === step.component);
      label.append(el('strong', '', step.title), el('small', '', component ? component.title : step.component));
      button.append(el('span', 'step-number', String(index + 1).padStart(2, '0')), label);
      button.addEventListener('click', () => { selectedStep = index; renderStep(); });
      item.append(button);
      diagram.append(item);
    });
    renderStep();
  }
  renderFlow();

  const metrics = list(data.metrics);
  metrics.forEach((metric) => {
    const card = el('article', 'metric-card');
    card.append(el('h4', '', metric.name), el('p', '', metric.summary), badge(`Область: ${text(metric.scope)}`), document.createTextNode(' '), badge(`Тип: ${text(metric.kind)}`));
    byId('metrics').append(card);
  });
  if (!metrics.length) empty(byId('metrics'), 'Каталог метрик не заполнен', 'Численные значения не подставляются автоматически.');

  const findings = list(data.findings);
  findings.forEach((finding) => {
    const card = el('article', 'finding-card');
    const row = el('div', 'badge-row');
    const severity = text(finding.severity, 'не указана');
    const critical = /^(critical|high|p0|p1|критическая|высокая)$/i.test(severity);
    row.append(badge(`Серьёзность: ${severity}`, critical ? 'badge-critical' : 'badge-warm'), badge(`Статус: ${text(finding.status)}`));
    card.append(row, el('h3', '', finding.title), facts([['Влияние', finding.impact], ['Рекомендация', finding.recommendation]]), sources(finding.sources));
    byId('findings').append(card);
  });
  if (!findings.length) empty(byId('findings'), 'Находки не подключены', 'Пустой список не означает отсутствие узких мест или завершённую квалификацию.');

  const reviews = list(data.reviews);
  reviews.forEach((review, index) => {
    const card = el('article', 'review-card');
    card.append(el('span', 'eyebrow', `ПЕРСПЕКТИВА ${String(index + 1).padStart(2, '0')}`), el('h3', '', review.role), el('p', '', review.summary));
    [['Сильные стороны', review.good, 'positive'], ['Вопросы и ограничения', review.concerns, 'concern']].forEach(([label, values, className]) => {
      card.append(el('h4', className, label));
      const ul = el('ul');
      const items = Array.isArray(values) ? values : [];
      items.forEach((value) => ul.append(el('li', '', value)));
      if (items.length) card.append(ul);
      else card.append(el('p', 'muted', 'Не указаны в разборе.'));
    });
    byId('reviews').append(card);
  });
  if (!reviews.length) empty(byId('reviews'), 'Ролевой разбор ещё не подключён', 'Ожидается массив reviews. Мнения не генерируются интерфейсом.');

  const importInput = byId('evidence-file');
  const clearButton = byId('clear-evidence');
  const status = byId('evidence-status');
  const evidence = byId('evidence-content');
  const numericFields = ['wall_seconds', 'user_cpu_seconds', 'system_cpu_seconds', 'peak_rss_bytes'];
  const stringFields = ['id', 'title', 'scope', 'revision', 'workload', 'outcome', 'limitations', 'source', 'flow_id', 'step_id'];
  let importGeneration = 0;
  const missing = (value) => value === undefined || value === null || value === '' || value === 'unknown';
  function validateEvidence(value) {
    if (!value || typeof value !== 'object' || Array.isArray(value)
      || value.format !== 'git-rs-architecture-evidence/v1' || !Array.isArray(value.items)) {
      throw new Error('Ожидается объект с format: git-rs-architecture-evidence/v1 и массивом items.');
    }
    if (value.items.length > 500) throw new Error('Допускается не более 500 записей.');
    return value.items.map((item, index) => {
      if (!item || typeof item !== 'object' || Array.isArray(item)) throw new Error(`Запись ${index + 1}: ожидается объект.`);
      const clean = {};
      stringFields.forEach((field) => {
        const entry = item[field];
        if (!missing(entry) && (typeof entry !== 'string' || entry.length > 16000)) {
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
      if (item.hotspots !== undefined && (!Array.isArray(item.hotspots) || item.hotspots.length > 50)) {
        throw new Error(`Запись ${index + 1}, hotspots: ожидается массив до 50 элементов.`);
      }
      clean.hotspots = (item.hotspots || []).map((hotspot, hotspotIndex) => {
        const location = `Запись ${index + 1}, hotspot ${hotspotIndex + 1}`;
        if (!hotspot || typeof hotspot !== 'object' || Array.isArray(hotspot)
          || typeof hotspot.name !== 'string' || hotspot.name.length > 16000
          || typeof hotspot.detail !== 'string' || hotspot.detail.length > 16000) {
          throw new Error(`${location}: name и detail должны быть строками до 16000 символов.`);
        }
        if (typeof hotspot.self_percent !== 'number' || !Number.isFinite(hotspot.self_percent)
          || hotspot.self_percent < 0 || hotspot.self_percent > 100) {
          throw new Error(`${location}: self_percent должен быть конечным числом от 0 до 100.`);
        }
        return { name: hotspot.name, self_percent: hotspot.self_percent, detail: hotspot.detail };
      });
      return clean;
    });
  }
  const measured = (value, unit) => value === null ? 'нет измерения' : `${value.toLocaleString('ru-RU', { maximumSignificantDigits: 12 })} ${unit}`;
  function evidenceCard(item) {
    const card = el('article', 'imported-evidence');
    card.append(el('h4', '', text(item.title, text(item.id, 'Без названия'))), facts([
      ['ID', item.id], ['Flow / step', `${text(item.flow_id, 'не указан')} / ${text(item.step_id, 'не указан')}`],
      ['Ревизия источника', text(item.revision, 'не указана')],
      ['Ревизия архитектурной карты', text(data.revision, 'не указана')],
      ['Scope / область', item.scope], ['Workload / нагрузка', item.workload],
      ['Исход', item.outcome],
      ['Ограничения', text(item.limitations, 'Не указаны — применимость не подтверждена')],
      ['Источник (текст)', item.source],
    ]));
    if (item.revision && data.revision && item.revision !== data.revision) {
      card.append(el('p', 'evidence-warning', 'Ревизия источника отличается от архитектурной карты. Evidence сохранено и показано; применимость требует проверки.'));
    }
    card.append(el('p', 'evidence-warning', 'Проверяйте область измерения и ограничения: результаты mapped core нельзя автоматически переносить на range-backed serving или production-нагрузку.'));
    card.append(facts(numericFields.map((field) => [field, measured(item[field], field === 'peak_rss_bytes' ? 'байт' : 'с')])));
    if (item.hotspots.length) {
      card.append(el('h4', '', 'Hotspots · self samples'));
      card.append(el('p', 'hotspot-note', 'доля CPU-clock samples в указанном запуске, не wall и не CPU процент production; вложенные категории не складывать'));
      const entries = el('ul', 'hotspot-list');
      item.hotspots.forEach((hotspot) => {
        const entry = el('li');
        const share = hotspot.self_percent.toLocaleString('ru-RU', { maximumSignificantDigits: 12 });
        entry.append(el('strong', '', hotspot.name), el('span', 'hotspot-share', `${share}% self samples`), el('p', 'muted', hotspot.detail));
        entries.append(entry);
      });
      card.append(entries);
    } else {
      card.append(el('p', 'muted', 'Hotspots не импортированы — нет измерения self samples.'));
    }
    return card;
  }
  function renderEvidence(items) {
    evidence.replaceChildren();
    if (!items.length) {
      empty(evidence, 'Нет локальных измерений', 'Выберите приватный JSON. Значения останутся только в памяти этой страницы.');
      return;
    }
    const scroll = el('div', 'table-scroll');
    scroll.tabIndex = 0;
    scroll.setAttribute('role', 'region');
    scroll.setAttribute('aria-label', 'Исторические и частичные evidence, таблица с горизонтальной прокруткой');
    const table = el('table');
    table.append(el('caption', '', 'Исторические / частичные доказательства · не текущий SLA и не production-квалификация'));
    const head = el('thead');
    const headings = el('tr');
    ['Запись / контекст', 'Wall, с', 'User CPU, с', 'System CPU, с', 'Peak RSS, байт', 'Результат', 'Ограничения', 'Источник (текст)'].forEach((label) => {
      const th = el('th', '', label);
      th.scope = 'col';
      headings.append(th);
    });
    head.append(headings);
    const body = el('tbody');
    items.forEach((item) => {
      const row = el('tr');
      const title = el('th', '', text(item.title, text(item.id, 'Без названия')));
      title.scope = 'row';
      const context = [
        `ID: ${text(item.id, 'не указан')}`,
        `Область: ${text(item.scope, 'не указана')}`,
        `Ревизия: ${text(item.revision, 'не указана')}`,
        `Нагрузка: ${text(item.workload, 'не указана')}`,
        item.revision && data.revision && item.revision !== data.revision ? 'Ревизия отличается от архитектурной карты.' : 'Совпадение ревизии само по себе не доказывает применимость.',
      ];
      title.append(el('span', 'evidence-context', context.join('\n')));
      row.append(title);
      numericFields.forEach((field) => row.append(el('td', '', measured(item[field], field === 'peak_rss_bytes' ? 'байт' : 'с'))));
      row.append(el('td', '', text(item.outcome, 'не указан')), el('td', '', text(item.limitations, 'Не указаны — применимость не подтверждена')), el('td', '', text(item.source, 'не указан')));
      body.append(row);
    });
    table.append(head, body);
    scroll.append(table);
    evidence.append(scroll);
    const profiles = el('div', 'evidence-profiles');
    profiles.append(el('h3', '', 'Контекст запусков и hotspots'));
    items.forEach((item) => profiles.append(evidenceCard(item)));
    evidence.append(profiles);
  }
  importInput.addEventListener('change', async () => {
    const generation = ++importGeneration;
    const file = importInput.files?.[0];
    if (!file) return;
    status.classList.remove('error');
    status.textContent = 'Чтение локального файла…';
    try {
      if (file.size > 2 * 1024 * 1024) throw new Error('Файл больше 2 МиБ.');
      const contents = await file.text();
      if (generation !== importGeneration) return;
      let parsed;
      try { parsed = JSON.parse(contents); }
      catch { throw new Error('Файл не является корректным JSON.'); }
      const items = validateEvidence(parsed);
      evidenceItems = items;
      renderEvidence(items);
      renderStep();
      clearButton.disabled = false;
      status.textContent = `Локально загружено записей: ${items.length}. Исторические / частичные доказательства; данные не отправлялись.`;
    } catch (error) {
      if (generation !== importGeneration) return;
      status.classList.add('error');
      status.textContent = `Импорт отклонён. ${error instanceof Error ? error.message : 'Не удалось прочитать файл.'} Предыдущие данные сохранены в памяти вкладки.`;
    } finally {
      if (generation === importGeneration) importInput.value = '';
    }
  });
  clearButton.addEventListener('click', () => {
    importGeneration += 1;
    importInput.value = '';
    evidenceItems = [];
    renderEvidence([]);
    renderStep();
    clearButton.disabled = true;
    status.classList.remove('error');
    status.textContent = 'Локальные evidence очищены из интерфейса. Файл на диске не изменён.';
  });
  renderEvidence([]);
})();
