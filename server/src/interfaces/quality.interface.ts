export const IQualityRepository = 'IQualityRepository';

export interface IQualityRepository {
  upsert(assetId: string, score: number): Promise<void>;
  clearAllIQAScores(): Promise<void>;
}
