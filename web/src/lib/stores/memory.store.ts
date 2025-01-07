import type { MemorylaneResponseDto } from '@immich/sdk';
import { writable } from 'svelte/store';

export const memoryStore = writable<MemorylaneResponseDto[]>();
