export const IQualityAssessmentRepository = 'IQualityAssessmentRepository';

export interface Distribution {
  mean: number;
  stddev: number;
}

export interface IQualityAssessmentRepository {
  upsert(assetId: string, score: number): Promise<void>;
  clearAllIQAScores(): Promise<void>;
  scoreDistribution(userIds: string[]): Promise<Distribution>;
}
