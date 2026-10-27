// Line buffer and SSE framing for the session log view. Run from web/ via `npm run check`.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
	appendLog,
	emptyPane,
	endReasonText,
	LOG_GAP_TEXT,
	LOG_LINE_CAP,
	LOG_RETRY_TEXT,
	LOG_UNAVAILABLE_TEXT,
	noteGap,
	readEventStream,
	sessionLogsExportURL,
	sessionLogsURL,
	SSEDecoder
} from './src/lib/logtext.ts';

test('splits lines and keeps a partial line', () => {
	const pane = emptyPane();
	const ids = { next: 1 };
	appendLog(pane, 'aa\r\nbb', ids);
	assert.deepEqual(
		pane.rows.map((row) => row.text),
		['aa']
	);
	assert.equal(pane.partial, 'bb');
	assert.equal(pane.lines, 1);
	appendLog(pane, '\n', ids);
	assert.deepEqual(
		pane.rows.map((row) => row.text),
		['aa', 'bb']
	);
	assert.equal(pane.partial, '');
	assert.equal(pane.lines, 2);
});

test('drops the oldest lines past the cap and counts the partial line', () => {
	const pane = emptyPane();
	const ids = { next: 1 };
	const body = Array.from({ length: LOG_LINE_CAP }, (_, i) => `n${i}`).join('\n') + '\n';
	appendLog(pane, body, ids);
	assert.equal(pane.lines, LOG_LINE_CAP);
	assert.equal(pane.rows[0]?.text, 'n0');
	appendLog(pane, 'zz', ids);
	assert.equal(pane.lines, LOG_LINE_CAP - 1);
	assert.equal(pane.partial, 'zz');
	assert.equal(pane.rows[0]?.text, 'n1');
	assert.equal(pane.rows[pane.rows.length - 1]?.text, `n${LOG_LINE_CAP - 1}`);
});

test('a gap finishes the open line and is not counted as a line', () => {
	const pane = emptyPane();
	const ids = { next: 1 };
	appendLog(pane, 'aa', ids);
	noteGap(pane, ids);
	assert.equal(pane.rows[0]?.kind, 'line');
	assert.equal(pane.rows[0]?.text, 'aa');
	assert.equal(pane.rows[1]?.kind, 'gap');
	assert.equal(pane.partial, '');
	assert.equal(pane.lines, 1);
	const full = emptyPane();
	appendLog(full, Array.from({ length: LOG_LINE_CAP }, (_, i) => `n${i}`).join('\n') + '\n', ids);
	noteGap(full, ids);
	appendLog(full, 'bb\n', ids);
	assert.equal(full.lines, LOG_LINE_CAP);
	assert.equal(full.rows.some((row) => row.kind === 'gap'), true);
	assert.equal(full.rows[0]?.text, 'n1');
	assert.equal(LOG_GAP_TEXT, 'some output was rotated out');
});

test('sse decoder reassembles chunks and skips keepalive', () => {
	const dec = new SSEDecoder();
	assert.deepEqual(dec.push('event: log\ndata: {"stream":"stdout","data":"aa"}'), []);
	const got = dec.push('\nid: stdout=a1;stderr=b1\n: keepalive\n\n');
	assert.equal(got.length, 1);
	assert.equal(got[0]?.event, 'log');
	assert.equal(got[0]?.data, '{"stream":"stdout","data":"aa"}');
	const split = new SSEDecoder();
	const first = split.push('event: gap\ndata: {"stream":');
	const second = split.push('"stderr"}\n\n');
	assert.equal(first.length, 0);
	assert.equal(second.length, 1);
	assert.equal(second[0]?.event, 'gap');
	assert.equal(second[0]?.data, '{"stream":"stderr"}');
});

test('readEventStream yields one event from split bytes', async () => {
	const encoder = new TextEncoder();
	const chunks = ['event: end\n', 'data: {"reason":"eof"}\n\n'];
	const body = new ReadableStream<Uint8Array>({
		start(controller) {
			for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
			controller.close();
		}
	});
	const seen: string[] = [];
	await readEventStream(body, (ev) => seen.push(`${ev.event}:${ev.data}`));
	assert.deepEqual(seen, ['end:{"reason":"eof"}']);
});

test('log urls and end reasons', () => {
	assert.equal(sessionLogsURL('abcdef', 200, false), '/api/v1/sessions/abcdef/logs?tail=200');
	assert.equal(
		sessionLogsURL('abcdef', 1000, true),
		'/api/v1/sessions/abcdef/logs?tail=1000&follow=true'
	);
	assert.equal(sessionLogsExportURL('abcdef'), '/api/v1/sessions/abcdef/logs/export');
	assert.equal(endReasonText('terminal'), '已結束');
	assert.equal(endReasonText('removed'), '已刪除');
	assert.equal(endReasonText('eof'), '已讀到結尾');
	assert.equal(endReasonText('other'), null);
	assert.equal(LOG_UNAVAILABLE_TEXT, '目前讀不到 log');
	assert.equal(LOG_RETRY_TEXT, '重試');
});
