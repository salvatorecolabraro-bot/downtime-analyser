'use strict';
self.importScripts('https://cdn.jsdelivr.net/npm/lz-string@1.4.4/libs/lz-string.min.js');

// Dedicated worker that parses a single file's content and returns parsed arrays

function parseContent(content, fileName) {
  const result = { lga: [], lge: [], lgd: [], lgdRestarts: [] };
  const rawLines = content.split('\n');
  let currentSection = null; // 'lga' | 'lge' | 'lgdEvents' | null
  const seenLga = new Set();
  const seenLge = new Set();
  const seenLgdEvents = new Set();

  for (let idx = 0; idx < rawLines.length; idx++) {
    const line = rawLines[idx].trim();

    // Enter sections when command line appears
    if (/^(?:lga|lgac)\s+-m\s+30d?/i.test(line)) { currentSection = 'lga'; continue; }
    if (/^(?:lge|lgec)\s+-m\s+30d?/i.test(line)) { currentSection = 'lge'; continue; }
    if (/^(?:lgd|lgdc)\s+-m\s+30d?/i.test(line)) { currentSection = 'lgdEvents'; continue; }

    // Exit section when prompt or another command appears
    if (/^[A-Z]{2}\w+>/.test(line) || /^(?:lga|lgac|lge|lgec|lgd|lgdc)\s+-m\s+\d+/i.test(line)) { currentSection = null; continue; }

    if (!currentSection) { continue; }

    // Skip header/decorative lines
    if (/^=+$/.test(line) || (/Timestamp/i.test(line) && !/^\d{4}-\d{2}-\d{2}/.test(line))) { continue; }
    if (!line) { continue; }

    // New format: semicolon-delimited
    if (line.includes(';')) {
      const parts = line.split(';').map(p => p.trim());
      // LGA/LGE with separate date/time: YYYY-MM-DD;HH:MM:SS;TYPE;SEV;OBJECT;TITLE;DETAIL
      if ((currentSection === 'lga' || currentSection === 'lge') &&
          parts.length >= 6 &&
          /^\d{4}-\d{2}-\d{2}$/.test(parts[0]) &&
          /^\d{2}:\d{2}:\d{2}$/.test(parts[1])) {
        const detail = (parts[6] || '').replace(/\s*SUPPRESSED\s*$/i, '').trim();
        const item = {
          fileName: fileName,
          dateIso: parts[0],
          time: parts[1],
          type: (parts[2] || '').replace(/\s+/g, ''),
          severity: parts[3] || '',
          object: parts[4] || '',
          title: parts[5] || '',
          detail: detail
        };
        const key = `${item.fileName}|${item.dateIso}|${item.time}|${item.type}|${item.severity}|${item.object}|${item.title}|${item.detail}`;
        if (currentSection === 'lga') {
          if (!seenLga.has(key)) { seenLga.add(key); result.lga.push(item); }
        } else {
          if (!seenLge.has(key)) { seenLge.add(key); result.lge.push(item); }
        }
        continue;
      }

      // LGD/LGDC events: YYYY-MM-DD;HH:MM:SS;Type/Reason;Value;Comment;Duration;...
      if (currentSection === 'lgdEvents' &&
          parts.length >= 6 &&
          /^\d{4}-\d{2}-\d{2}$/.test(parts[0]) &&
          /^\d{2}:\d{2}:\d{2}$/.test(parts[1])) {
        const ev = {
          fileName: fileName,
          dateIso: parts[0],
          time: parts[1],
          typeReason: parts[2] || '',
          value: parts[3] || '',
          comment: parts[4] || '',
          duration: parts[5] || ''
        };
        const key = `${ev.fileName}|${ev.dateIso}|${ev.time}|${ev.typeReason}|${ev.value}|${ev.comment}|${ev.duration}`;
        if (!seenLgdEvents.has(key)) { seenLgdEvents.add(key); result.lgdRestarts.push(ev); }
        continue;
      }
    }

    // Fallback: old space-delimited format
    const match = line.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(AL|EV)\s+([*mMw])\s+(.+)$/);
    if (match) {
      const item = {
        fileName: fileName,
        dateIso: match[1],
        time: match[2],
        type: match[3],
        severity: match[4],
        object: '',
        title: match[5].trim(),
        detail: ''
      };
      const key = `${item.fileName}|${item.dateIso}|${item.time}|${item.type}|${item.severity}|${item.object}|${item.title}|${item.detail}`;
      if (currentSection === 'lga') {
        if (!seenLga.has(key)) { seenLga.add(key); result.lga.push(item); }
      } else if (currentSection === 'lge') {
        if (!seenLge.has(key)) { seenLge.add(key); result.lge.push(item); }
      }
    }
  }

  // Parse LGD/LGDC statistics (support both old and new formats)
  // Prefer scanning lines to catch semicolon-delimited rows as well
  const allLines = content.split('\n').map(l => l.trim()).filter(l => l);
  allLines.forEach(line => {
    if (line.startsWith('Number Of outages') ||
        line.startsWith('Total downtime') ||
        line.startsWith('Downtime per day') ||
        line.startsWith('Downtime per outage')) {
      if (line.includes(';')) {
        const parts = line.split(';').map(p => p.trim());
        if (parts.length >= 6) {
          result.lgd.push({
            fileName: fileName,
            metric: parts[0],
            nodeUpgrade: parts[1],
            nodeManual: parts[2],
            nodeSpontaneous: parts[3],
            allNodeRestarts: parts[4],
            partialOutages: parts[5]
          });
        }
      } else {
        const parts = line.split(/\s{2,}/);
        if (parts.length >= 6) {
          result.lgd.push({
            fileName: fileName,
            metric: parts[0].trim(),
            nodeUpgrade: parts[1].trim(),
            nodeManual: parts[2].trim(),
            nodeSpontaneous: parts[3].trim(),
            allNodeRestarts: parts[4].trim(),
            partialOutages: parts[5].trim()
          });
        }
      }
    }
  });

  // Parse LGD Restart Events Table (support semicolon-delimited rows) - fallback
  const lgdRestartMatch = content.match(/=+\s*Timestamp \(UTC\)\s+RestartType\/Reason\s+SwVersion\s+SwRelease\s+RCS Downtime\s+Appl\. Downtime\s+TN Downtime\s+RATs Downtime\s*=+([\s\S]*?)(?=Node uptime since last restart:|$)/i);
  if (lgdRestartMatch) {
    const restartContent = lgdRestartMatch[1];
    const restartLines = restartContent.split('\n').filter(line => line.trim() && !line.match(/^=+$/));
    restartLines.forEach(line => {
      if (line.includes(';')) {
        const parts = line.split(';').map(p => p.trim());
        let dateIso = '', time = '';
        if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$/.test(parts[0])) {
          dateIso = parts[0].slice(0,10);
          time = parts[0].slice(11);
          const ev = {
            fileName: fileName,
            dateIso,
            time,
            typeReason: parts[1] || '',
            value: parts[2] || '',
            comment: parts[3] || '',
            duration: parts[4] || ''
          };
          const key = `${ev.fileName}|${ev.dateIso}|${ev.time}|${ev.typeReason}|${ev.value}|${ev.comment}|${ev.duration}`;
          if (!seenLgdEvents.has(key)) { seenLgdEvents.add(key); result.lgdRestarts.push(ev); }
          return;
        }
        if (parts.length >= 6 && /^\d{4}-\d{2}-\d{2}$/.test(parts[0]) && /^\d{2}:\d{2}:\d{2}$/.test(parts[1])) {
          dateIso = parts[0];
          time = parts[1];
          const ev2 = {
            fileName: fileName,
            dateIso,
            time,
            typeReason: parts[2] || '',
            value: parts[3] || '',
            comment: parts[4] || '',
            duration: parts[5] || ''
          };
          const key2 = `${ev2.fileName}|${ev2.dateIso}|${ev2.time}|${ev2.typeReason}|${ev2.value}|${ev2.comment}|${ev2.duration}`;
          if (!seenLgdEvents.has(key2)) { seenLgdEvents.add(key2); result.lgdRestarts.push(ev2); }
          return;
        }
      }

      // Old format parsing fallback
      const timestampMatch = line.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(.+)$/);
      if (timestampMatch) {
        const dateIso = timestampMatch[1];
        const time = timestampMatch[2];
        const restOfLine = timestampMatch[3];
        const reasonMatch = restOfLine.match(/^([^)]+\))\s*(.*)$/);
        if (reasonMatch) {
          const typeReason = reasonMatch[1];
          const remainingFields = reasonMatch[2].trim();
          const fields = remainingFields.split(/\s{2,}/);
          const ev3 = {
            fileName: fileName,
            dateIso,
            time,
            typeReason,
            value: fields[0] || '',
            comment: fields[1] || '',
            duration: fields[2] || ''
          };
          const key3 = `${ev3.fileName}|${ev3.dateIso}|${ev3.time}|${ev3.typeReason}|${ev3.value}|${ev3.comment}|${ev3.duration}`;
          if (!seenLgdEvents.has(key3)) { seenLgdEvents.add(key3); result.lgdRestarts.push(ev3); }
        }
      }
    });
  }

  // Global scan: capture semicolon-separated rows anywhere in the file
  for (let i = 0; i < rawLines.length; i++) {
    const line = rawLines[i].trim();
    if (!line || !line.includes(';') || /^=+$/.test(line)) continue;
    const parts = line.split(';').map(p => p.trim());
    if (parts.length < 6) continue;
    const isDate = /^\d{4}-\d{2}-\d{2}$/.test(parts[0]);
    const isTime = /^\d{2}:\d{2}:\d{2}$/.test(parts[1]);
    if (!isDate || !isTime) continue;

    const typeField = (parts[2] || '').replace(/\s+/g, '');
    if (typeField === 'AL' || typeField === 'EV') {
      const detail = (parts[6] || '').replace(/\s*SUPPRESSED\s*$/i, '').trim();
      const item = {
        fileName: fileName,
        dateIso: parts[0],
        time: parts[1],
        type: typeField,
        severity: parts[3] || '',
        object: parts[4] || '',
        title: parts[5] || '',
        detail: detail
      };
      const key = `${item.fileName}|${item.dateIso}|${item.time}|${item.type}|${item.severity}|${item.object}|${item.title}|${item.detail}`;
      if (typeField === 'AL') {
        if (!seenLga.has(key)) { seenLga.add(key); result.lga.push(item); }
      } else {
        if (!seenLge.has(key)) { seenLge.add(key); result.lge.push(item); }
      }
    } else {
      // Treat as LGD Event if not AL/EV
      const ev = {
        fileName: fileName,
        dateIso: parts[0],
        time: parts[1],
        typeReason: parts[2] || '',
        value: parts[3] || '',
        comment: parts[4] || '',
        duration: parts[5] || ''
      };
      const key = `${ev.fileName}|${ev.dateIso}|${ev.time}|${ev.typeReason}|${ev.value}|${ev.comment}|${ev.duration}`;
      if (!seenLgdEvents.has(key)) { seenLgdEvents.add(key); result.lgdRestarts.push(ev); }
    }
  }

  return result;
}

self.onmessage = function(e) {
  const data = e.data || {};
  if (data.type === 'parse') {
    try {
      const res = parseContent(data.content || '', data.fileName || '');
      self.postMessage({ id: data.id, status: 'ok', result: res });
    } catch (err) {
      self.postMessage({ id: data.id, status: 'error', error: (err && err.message) ? err.message : String(err) });
    }
  } else if (data.type === 'compress') {
    try {
      let compressed = null; let format = 'json';
      if (typeof LZString !== 'undefined' && LZString.compressToUTF16) {
        compressed = LZString.compressToUTF16(String(data.content || ''));
        format = 'lzutf16';
      }
      self.postMessage({ id: data.id, status: 'ok', compressed: compressed || String(data.content || ''), format });
    } catch (err) {
      self.postMessage({ id: data.id, status: 'error', error: (err && err.message) ? err.message : String(err) });
    }
  }
};