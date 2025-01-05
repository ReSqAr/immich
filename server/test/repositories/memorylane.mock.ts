import { IMemorylaneRepository } from 'src/interfaces/memorylane.interface';
import { Mocked, vitest } from 'vitest';

export const newMemorylaneRepositoryMock = (): Mocked<IMemorylaneRepository> => {
  return {
    refresh: vitest.fn(),
    cluster: vitest.fn(),
    recentHighlight: vitest.fn(),
    person: vitest.fn(),
    year: vitest.fn(),
  };
};
