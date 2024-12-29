export const IQualityAssessmentRepository = 'IQualityAssessmentRepository';

export interface IQualityAssessmentRepository {
  upsert(assetId: string, score: number): Promise<void>;
  clearAllIQAScores(): Promise<void>;
}
