import { IQualityAssessmentRepository } from 'src/interfaces/quality-assessment.interface';
import { Mocked, vitest } from 'vitest';

export const newQualityAssessmentRepositoryMock = (): Mocked<IQualityAssessmentRepository> => {
  return {
    upsert: vitest.fn(),
    clearAllIQAScores: vitest.fn(),
    scoreDistribution: vitest.fn(),
  };
};
