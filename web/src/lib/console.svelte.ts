// Global console state (D1077): signed-in user, region availability, controller_absent.
import { api, failureOf, setUnauthorizedHandler, type Freshness, type Me } from './api';

class ConsoleState {
	user = $state<Me | null>(null);
	auth = $state<'unknown' | 'in' | 'out'>('unknown');
	meError = $state<string | null>(null);
	unavailableRegions = $state<string[]>([]);
	controllerAbsent = $state(false);

	applyFreshness(freshness: Freshness | undefined): void {
		if (!freshness) return;
		this.unavailableRegions = freshness.unavailable_regions;
		this.controllerAbsent = freshness.controller_absent;
	}

	markSignedOut(): void {
		this.user = null;
		this.auth = 'out';
		this.meError = null;
		this.unavailableRegions = [];
		this.controllerAbsent = false;
	}

	async loadMe(): Promise<void> {
		this.meError = null;
		try {
			const result = await api.GET('/auth/me');
			if (result.response.status === 401) {
				this.markSignedOut();
				return;
			}
			const failure = failureOf(result.error, result.response);
			if (failure || !result.data) {
				this.meError = failure?.message ?? 'Empty response';
				return;
			}
			this.user = result.data;
			this.auth = 'in';
		} catch {
			this.meError = 'API unreachable';
		}
	}

	async logout(): Promise<void> {
		try {
			const result = await api.POST('/auth/logout');
			if (result.response.status === 204 || result.response.status === 401) {
				this.markSignedOut();
				return;
			}
			const failure = failureOf(result.error, result.response);
			this.meError = failure?.message ?? `HTTP ${result.response.status}`;
		} catch {
			this.meError = 'API unreachable';
		}
	}
}

export const consoleState = new ConsoleState();

setUnauthorizedHandler(() => {
	consoleState.markSignedOut();
});
