import { IQualityRepository } from 'src/interfaces/quality.interface';
import { Mocked, vitest } from 'vitest';

export const newQualityRepositoryMock = (): Mocked<IQualityRepository> => {
  return {
    upsert: vitest.fn(),
    clearAllIQAScores: vitest.fn(),
  };
};
