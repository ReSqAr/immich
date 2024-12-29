export const IMemorylaneRepository = 'IMemorylaneRepository';

interface Memorylane {
  title: string;
  assetIds: string[];
}

export interface IMemorylaneRepository {
  refresh(): Promise<void>;
  recentHighlight(userIds: string[], seed: number, limit: number): Promise<Memorylane>;
  cluster(userIds: string[], seed: number, limit: number): Promise<Memorylane>;
}
