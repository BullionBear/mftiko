// Session wizard draft: the inputs rows (E15-D023). Run from web/ via `npm run check`.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
	applyProblem,
	emptyDraft,
	firstErrorStep,
	localErrors,
	sessionRequestBody,
	stepForPath,
	type Draft
} from './src/lib/session-draft.ts';

const digest = `sha256:${'1'.repeat(64)}`;

function withInput(): Draft {
	const draft = emptyDraft();
	draft.name = 'inputs';
	draft.stsInstance = 'sts1';
	draft.repository = 'registry:5000/strategies/hello';
	draft.strategyRef = 'accept';
	draft.inputs = [{ key: 1, name: 'model', repository: ' registry:5000/data/model ', digest: ` ${digest} ` }];
	return draft;
}

test('inputs are sent with repository and digest trimmed', () => {
	const body = JSON.parse(sessionRequestBody(withInput()));
	assert.deepEqual(body.inputs, [{ name: 'model', repository: 'registry:5000/data/model', digest }]);
	assert.equal('inputs' in JSON.parse(sessionRequestBody(emptyDraft())), false);
});

test('an input needs a name, a repository and a sha256 digest', () => {
	const draft = withInput();
	assert.deepEqual(localErrors(draft), {});
	draft.inputs.push({ key: 2, name: 'model', repository: ' ', digest: 'latest' });
	draft.inputs.push({ key: 3, name: ' ', repository: 'registry:5000/data/vocab', digest: '' });
	draft.inputs.push({ key: 4, name: 'upper', repository: 'registry:5000/data/upper', digest: `sha256:${'A'.repeat(64)}` });
	assert.deepEqual(localErrors(draft), {
		'/inputs/1/name': 'duplicate',
		'/inputs/1/repository': 'required',
		'/inputs/1/digest': 'must be a sha256 digest',
		'/inputs/2/name': 'required',
		'/inputs/2/digest': 'required',
		'/inputs/3/digest': 'must be a sha256 digest'
	});
	assert.equal(firstErrorStep(localErrors(draft)), 4);
});

test('server errors on an input land on its row; a row that is gone goes to the banner', () => {
	const mapped = applyProblem(
		{
			type: 'about:blank',
			title: 'Validation failed',
			status: 400,
			code: 'validation_failed',
			errors: [
				{ path: '/inputs/0/repository', reason: 'is not allowed' },
				{ path: '/inputs/0/digest', reason: 'is too large' },
				{ path: '/inputs/1/digest', reason: 'is not a data artifact' }
			]
		},
		withInput()
	);
	assert.deepEqual(mapped.fields, { '/inputs/0/repository': 'is not allowed', '/inputs/0/digest': 'is too large' });
	assert.deepEqual(mapped.banner, ['/inputs/1/digest: is not a data artifact']);
});

test('steps: inputs after accounts, then limits, then confirm', () => {
	assert.equal(stepForPath('/accounts/0/api_id'), 3);
	assert.equal(stepForPath('/inputs'), 4);
	assert.equal(stepForPath('/inputs/0/digest'), 4);
	assert.equal(stepForPath('/limits/memory_mb'), 5);
	assert.equal(stepForPath('/grace_ms'), 5);
	assert.equal(stepForPath('/labels'), 6);
});
