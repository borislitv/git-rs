'use strict';

// Only component transitions present in steps become edges; layout is not a trace.
(() => {
  const NS = 'http://www.w3.org/2000/svg';
  // Stable visual layers, not inferred authority, dependencies or execution timing.
  const LAYERS = [
    ['http', 'ssh'],
    ['admission', 'history', 'maintenance'],
    ['metadata'],
    ['upload', 'receive', 'blame'],
    ['object-core', 'derived-cache'],
    ['source', 'source-cache'],
    ['storage'],
    ['forks', 'observability'],
  ];
  let instance = 0;
  const text = (value, fallback = '') => typeof value === 'string' ? value : fallback;
  const list = (value) => Array.isArray(value) ? value.filter((v) => v && typeof v === 'object') : [];
  const html = (tag, className, content) => {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (content !== undefined) node.textContent = content;
    return node;
  };
  const svgEl = (tag, attrs = {}, content) => {
    const node = document.createElementNS(NS, tag);
    Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, String(value)));
    if (content !== undefined) node.textContent = content;
    return node;
  };
  const words = (value, width) => {
    const lines = [];
    let line = '';
    for (const word of value.split(/\s+/)) {
      if (line && line.length + word.length + 1 > width) {
        lines.push(line);
        line = '';
      }
      // Also wrap an unfamiliar, unbroken identifier without losing its text.
      let rest = word;
      while (rest.length > width) {
        if (line) { lines.push(line); line = ''; }
        lines.push(rest.slice(0, width));
        rest = rest.slice(width);
      }
      line = line ? `${line} ${rest}` : rest;
    }
    if (line) lines.push(line);
    return lines;
  };

  function mount(root, options = {}) {
    const prefix = `grg-${++instance}`;
    const components = list(options.components).filter((c) => text(c.id));
    const flows = list(options.flows).filter((f) => text(f.id));
    const componentMap = new Map(components.map((c) => [c.id, c]));
    const nodes = new Map();
    const edges = new Map();
    const routes = new Map();
    let selectedId = '';
    let activeFlow = null;
    let scale = 1;
    let drag = null;
    let suppressClick = false;
    const width = 1140;
    const nodeWidth = 292;
    let nextY = 48;

    root.classList.add('grg');
    const heading = html('h3', 'grg-heading', 'Логическая карта путей');
    heading.id = `${prefix}-heading`;
    const note = html('p', 'grg-note', 'Сверху вниз — расположение компонентов, не временная шкала и не параллельная трасса. Стрелки — соседние переходы из шагов; развилки и возвраты сохраняются в описании пути.');
    const toolbar = html('div', 'grg-toolbar');
    const flowLabel = html('label', 'grg-field', 'Путь запроса');
    const picker = html('select', 'grg-flow');
    const allOption = html('option', '', 'Все пути');
    allOption.value = '';
    picker.append(allOption);
    flows.forEach((flow, index) => {
      const option = html('option', '', text(flow.title, flow.id));
      option.value = String(index);
      picker.append(option);
    });
    flowLabel.append(picker);
    const searchLabel = html('label', 'grg-field', 'Найти компонент');
    const search = html('input', 'grg-search');
    search.type = 'search';
    search.placeholder = 'Название или ID';
    search.autocomplete = 'off';
    searchLabel.append(search);
    const controls = html('div', 'grg-controls');
    controls.setAttribute('role', 'group');
    controls.setAttribute('aria-label', 'Масштаб карты');
    const button = (label, action, ariaLabel) => {
      const node = html('button', 'grg-button', label);
      node.type = 'button';
      if (ariaLabel) node.setAttribute('aria-label', ariaLabel);
      node.addEventListener('click', action);
      return node;
    };
    const zoomOut = button('−', () => zoom(scale / 1.25), 'Уменьшить карту');
    const zoomIn = button('+', () => zoom(scale * 1.25), 'Увеличить карту');
    const zoomLabel = html('output', 'grg-zoom', '100%');
    controls.append(zoomOut, zoomLabel, zoomIn,
      button('Вместить', () => zoom(Math.min(viewport.clientWidth / width, viewport.clientHeight / height), true)),
      button('Сброс', () => zoom(1, true)));
    toolbar.append(flowLabel, searchLabel, controls);
    const status = html('p', 'grg-status');
    status.setAttribute('role', 'status');
    const help = html('p', 'grg-note', 'Перетаскивайте фон или прокручивайте карту. Tab — узлы и управление; стрелки — соседний узел по расположению; Enter / пробел — выбрать; Home / End — первый / последний узел. Поиск: Enter переходит к первому совпадению.');
    help.id = `${prefix}-help`;
    const viewport = html('div', 'grg-viewport');
    viewport.setAttribute('role', 'region');
    viewport.setAttribute('aria-labelledby', heading.id);
    viewport.setAttribute('aria-describedby', help.id);
    viewport.tabIndex = 0;
    const svg = svgEl('svg', { class: 'grg-svg', role: 'group', 'aria-label': 'Компоненты и направленные переходы запросов' });
    const defs = svgEl('defs');
    ['normal', 'active'].forEach((kind) => {
      const marker = svgEl('marker', { id: `${prefix}-arrow-${kind}`, viewBox: '0 0 10 10', refX: 9, refY: 5, markerWidth: 8, markerHeight: 8, orient: 'auto', markerUnits: 'userSpaceOnUse' });
      marker.append(svgEl('path', { d: 'M 0 0 L 10 5 L 0 10 z', class: `grg-arrow grg-arrow-${kind}` }));
      defs.append(marker);
    });
    const edgeLayer = svgEl('g', { class: 'grg-edges' });
    const nodeLayer = svgEl('g', { class: 'grg-nodes' });
    svg.append(defs, edgeLayer, nodeLayer);
    viewport.append(svg);
    const routePanel = html('div', 'grg-route');
    root.replaceChildren(heading, note, toolbar, status, help, viewport, routePanel);

    const known = new Set(LAYERS.flat());
    const unknown = components.filter((c) => !known.has(c.id)).map((c) => c.id);
    const layers = [...LAYERS, ...Array.from({ length: Math.ceil(unknown.length / 3) }, (_, i) => unknown.slice(i * 3, i * 3 + 3))];
    layers.forEach((ids) => {
      const row = ids.filter((id) => componentMap.has(id));
      if (!row.length) return;
      let rowHeight = 0;
      row.forEach((id, index) => {
        const component = componentMap.get(id);
        const lines = words(text(component.title, id), 29);
        const h = Math.max(118, 62 + lines.length * 22);
        const x = (width - row.length * 352 + 60) / 2 + index * 352;
        nodes.set(id, { component, x, y: nextY, h, lines });
        rowHeight = Math.max(rowHeight, h);
      });
      nextY += rowHeight + 96;
    });
    const height = Math.max(300, nextY);
    svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
    svg.setAttribute('width', width);
    svg.setAttribute('height', height);

    flows.forEach((flow) => {
      const stages = [];
      list(flow.steps).forEach((step, index) => {
        const last = stages[stages.length - 1];
        const entry = { step, number: index + 1 };
        if (last && last.id === step.component) last.entries.push(entry);
        else stages.push({ id: step.component, entries: [entry] });
      });
      routes.set(flow, stages);
      stages.slice(1).forEach((to, index) => {
        const from = stages[index];
        // Missing components remain in the textual route; never bridge across them.
        if (!nodes.has(from.id) || !nodes.has(to.id)) return;
        const key = JSON.stringify([from.id, to.id]);
        if (!edges.has(key)) edges.set(key, { from: from.id, to: to.id, occurrences: [] });
        edges.get(key).occurrences.push({ flow, from, to });
      });
    });
    const describeStage = (stage) => {
      const title = text(componentMap.get(stage.id)?.title, text(stage.id, 'Компонент не указан'));
      return `${title}: ${stage.entries.map(({ step, number }) => `шаг ${number}, ${text(step.title, step.id)}`).join('; ')}`;
    };
    let edgeIndex = 0;
    edges.forEach((edge) => {
      const index = edgeIndex++;
      const from = nodes.get(edge.from);
      const to = nodes.get(edge.to);
      const down = to.y > from.y;
      const startX = from.x + nodeWidth / 2;
      const endX = to.x + nodeWidth / 2;
      const startY = down ? from.y + from.h : from.y;
      const endY = down ? to.y : to.y + to.h;
      let d;
      if (down && to.y - (from.y + from.h) < 160) {
        const middle = (startY + endY) / 2;
        d = `M ${startX} ${startY} C ${startX} ${middle}, ${endX} ${middle}, ${endX} ${endY - 4}`;
      } else {
        // Long and returning transitions use outer gutters rather than cross nodes.
        const gutter = index % 2 ? 18 + (index % 5) * 5 : width - 18 - (index % 5) * 5;
        const offset = down ? 30 : -30;
        d = `M ${startX} ${startY} L ${startX} ${startY + offset} L ${gutter} ${startY + offset} L ${gutter} ${endY - offset} L ${endX} ${endY - offset} L ${endX} ${endY - (down ? 4 : -4)}`;
      }
      edge.path = svgEl('path', { d, class: 'grg-edge', 'marker-end': `url(#${prefix}-arrow-normal)` });
      edge.label = svgEl('title');
      edge.path.append(edge.label);
      edgeLayer.append(edge.path);
    });

    function reveal(node) {
      const padding = 24;
      const x = node.x * scale;
      const y = node.y * scale;
      if (x < viewport.scrollLeft || x + nodeWidth * scale > viewport.scrollLeft + viewport.clientWidth) {
        viewport.scrollLeft = Math.max(0, x - padding);
      }
      if (y < viewport.scrollTop || y + node.h * scale > viewport.scrollTop + viewport.clientHeight) {
        viewport.scrollTop = Math.max(0, y - padding);
      }
    }
    function selectComponent(id) {
      if (!nodes.has(id)) return;
      selectedId = id;
      nodes.forEach((node, key) => node.element.setAttribute('aria-pressed', String(key === id)));
      reveal(nodes.get(id));
    }
    const choose = (id) => {
      selectComponent(id);
      if (typeof options.onComponent === 'function') options.onComponent(id);
    };
    function focusNode(id) {
      const node = nodes.get(id);
      if (!node) return;
      node.element.focus({ preventScroll: true });
      reveal(node);
    }
    nodes.forEach((node, id) => {
      const group = svgEl('g', { class: 'grg-node', transform: `translate(${node.x} ${node.y})`, role: 'button', tabindex: 0, 'aria-pressed': 'false', 'data-component': id });
      group.append(svgEl('title', {}, `${text(node.component.title, id)}. ${text(node.component.summary)}`));
      group.append(svgEl('rect', { width: nodeWidth, height: node.h, rx: 14 }));
      group.append(svgEl('text', { x: 18, y: 27, class: 'grg-node-id' }, id));
      const label = svgEl('text', { x: 18, y: 55, class: 'grg-node-title', 'aria-hidden': 'true' });
      node.lines.forEach((line, index) => label.append(svgEl('tspan', { x: 18, dy: index ? 22 : 0 }, line)));
      group.append(label);
      group.addEventListener('click', () => { if (!suppressClick) choose(id); });
      group.addEventListener('focus', () => reveal(node));
      group.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          choose(id);
          return;
        }
        const ids = [...nodes.keys()];
        let target;
        if (event.key === 'Home') target = ids[0];
        if (event.key === 'End') target = ids[ids.length - 1];
        const direction = { ArrowDown: [0, 1], ArrowUp: [0, -1], ArrowLeft: [-1, 0], ArrowRight: [1, 0] }[event.key];
        if (direction) {
          const candidates = [...nodes.entries()].map(([key, other]) => {
            const dx = other.x - node.x;
            const dy = other.y - node.y;
            const forward = dx * direction[0] + dy * direction[1];
            const sideways = Math.abs(dx * direction[1] - dy * direction[0]);
            return { key, forward, distance: forward + sideways * 3 };
          }).filter((item) => item.forward > 0).sort((a, b) => a.distance - b.distance);
          target = candidates[0]?.key;
        }
        if (direction || target) event.preventDefault();
        if (target) focusNode(target);
      });
      node.element = group;
      nodeLayer.append(group);
    });

    function update() {
      const stages = activeFlow ? routes.get(activeFlow) : [];
      const participating = new Set(stages.map((stage) => stage.id));
      const query = search.value.trim().toLocaleLowerCase('ru');
      let matches = 0;
      nodes.forEach((node, id) => {
        const visits = stages.filter((stage) => stage.id === id);
        const matching = !query || `${id} ${text(node.component.title)}`.toLocaleLowerCase('ru').includes(query);
        if (matching) matches += 1;
        node.element.classList.toggle('grg-participating', participating.has(id));
        node.element.classList.toggle('grg-inactive', Boolean(activeFlow) && !participating.has(id));
        node.element.classList.toggle('grg-match', Boolean(query) && matching);
        node.element.setAttribute('aria-label', `${text(node.component.title, id)}. ${visits.length ? visits.map(describeStage).join('. ') : activeFlow ? 'Не указан в шагах выбранного пути.' : text(node.component.summary)}${query && matching ? '. Совпадение поиска.' : ''}`);
      });
      edges.forEach((edge) => {
        const occurrences = edge.occurrences.filter((o) => !activeFlow || o.flow === activeFlow);
        const active = Boolean(activeFlow) && occurrences.length > 0;
        edge.path.classList.toggle('grg-edge-active', active);
        edge.path.classList.toggle('grg-edge-inactive', Boolean(activeFlow) && !active);
        edge.path.setAttribute('marker-end', `url(#${prefix}-arrow-${active ? 'active' : 'normal'})`);
        edge.label.textContent = (occurrences.length ? occurrences : edge.occurrences).map((o) => `${text(o.flow.title, o.flow.id)}: ${describeStage(o.from)} → ${describeStage(o.to)}`).join('\n');
        if (active) edgeLayer.append(edge.path);
      });
      status.textContent = `${components.length} компонентов · ${activeFlow ? text(activeFlow.title, activeFlow.id) : `${flows.length} путей, ${edges.size} направленных связей`}${query ? ` · найдено: ${matches}` : ''}.`;
    }
    function renderRoute() {
      routePanel.replaceChildren();
      if (!activeFlow) {
        routePanel.append(html('p', 'grg-note', 'Все компоненты остаются на карте, в том числе не названные отдельным шагом. Отсутствие стрелки не доказывает отсутствие зависимости. Выберите путь, чтобы увидеть последовательность, проверки, копии и лимиты.'));
        return;
      }
      const flow = activeFlow;
      routePanel.append(html('h4', '', text(flow.title, flow.id)), html('p', 'grg-note', text(flow.summary)));
      const stages = routes.get(flow);
      routePanel.append(html('p', 'grg-route-summary', stages.map((stage) => `${text(componentMap.get(stage.id)?.title, text(stage.id, 'Не указан'))} [${stage.entries.map((e) => e.number).join(', ')}]`).join(' → ')));
      const open = button('Разобрать шаги', () => { if (typeof options.onFlow === 'function') options.onFlow(flow.id); });
      open.disabled = typeof options.onFlow !== 'function';
      routePanel.append(open);
      const details = html('details', 'grg-route-details');
      details.append(html('summary', '', 'Последовательность и стоимость шагов (из архитектурных данных)'));
      const sequence = html('ol', 'grg-sequence');
      stages.forEach((stage) => {
        const item = html('li');
        const title = text(componentMap.get(stage.id)?.title, text(stage.id, 'Компонент не указан'));
        if (nodes.has(stage.id)) item.append(button(title, () => { choose(stage.id); focusNode(stage.id); }));
        else item.append(html('strong', '', title));
        stage.entries.forEach(({ step, number }) => {
          const entry = html('details', 'grg-step');
          entry.append(html('summary', '', `Шаг ${number}: ${text(step.title, step.id)}`));
          const facts = html('dl');
          [['Проверки', 'checks'], ['Копии и владение', 'copies'], ['Лимиты и admission', 'limits'], ['CPU', 'cpu'], ['RAM / кеш', 'memory'], ['Время', 'time'], ['I/O', 'io']].forEach(([label, field]) => {
            const value = step[field];
            facts.append(html('dt', '', label), html('dd', '', Array.isArray(value) ? value.map((v) => text(v)).join('\n') : text(value, 'Нет описания в данных')));
          });
          entry.append(facts);
          item.append(entry);
        });
        sequence.append(item);
      });
      details.append(sequence);
      routePanel.append(details);
    }
    function zoom(value, reset = false) {
      const old = scale;
      scale = Math.max(0.12, Math.min(2.5, value));
      const centerX = (viewport.scrollLeft + viewport.clientWidth / 2) / old;
      const centerY = (viewport.scrollTop + viewport.clientHeight / 2) / old;
      svg.setAttribute('width', Math.round(width * scale));
      svg.setAttribute('height', Math.round(height * scale));
      viewport.scrollLeft = reset ? 0 : centerX * scale - viewport.clientWidth / 2;
      viewport.scrollTop = reset ? 0 : centerY * scale - viewport.clientHeight / 2;
      zoomLabel.textContent = `${Math.round(scale * 100)}%`;
      zoomOut.disabled = scale <= 0.12;
      zoomIn.disabled = scale >= 2.5;
    }
    picker.addEventListener('change', () => {
      activeFlow = picker.value === '' ? null : flows[Number(picker.value)];
      update();
      renderRoute();
    });
    function showAllPaths() {
      picker.value = '';
      activeFlow = null;
      update();
      renderRoute();
    }
    search.addEventListener('input', update);
    search.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter') return;
      const match = [...nodes.entries()].find(([, node]) => node.element.classList.contains('grg-match'));
      if (match) { event.preventDefault(); focusNode(match[0]); }
    });
    viewport.addEventListener('pointerdown', (event) => {
      suppressClick = false;
      if (event.button !== 0 || event.target.closest('.grg-node')) return;
      drag = { id: event.pointerId, x: event.clientX, y: event.clientY, left: viewport.scrollLeft, top: viewport.scrollTop };
      viewport.setPointerCapture(event.pointerId);
      viewport.classList.add('grg-dragging');
    });
    viewport.addEventListener('pointermove', (event) => {
      if (!drag || drag.id !== event.pointerId) return;
      const dx = event.clientX - drag.x;
      const dy = event.clientY - drag.y;
      if (Math.abs(dx) + Math.abs(dy) > 5) suppressClick = true;
      viewport.scrollLeft = drag.left - dx;
      viewport.scrollTop = drag.top - dy;
    });
    const endDrag = (event) => {
      if (!drag || drag.id !== event.pointerId) return;
      drag = null;
      viewport.classList.remove('grg-dragging');
      if (viewport.hasPointerCapture(event.pointerId)) viewport.releasePointerCapture(event.pointerId);
    };
    viewport.addEventListener('pointerup', endDrag);
    viewport.addEventListener('pointercancel', endDrag);
    viewport.addEventListener('lostpointercapture', endDrag);
    update();
    renderRoute();
    return { selectComponent, showAllPaths };
  }

  globalThis.GitRsGraph = Object.freeze({ mount });
})();
