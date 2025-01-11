export const IMemorylaneRepository = 'IMemorylaneRepository';

interface Memorylane {
  assetIds: string[];
}

export interface MemoryLaneCluster extends Memorylane {
  clusterID: number | undefined;
  startDate: Date | undefined;
  endDate: Date | undefined;
  locations: string[];
}

export interface MemoryLanePerson extends Memorylane {
  personName: string | undefined;
}

export type MemoryLaneRecentHighlights = Memorylane;

export interface MemoryLaneYear extends Memorylane {
  year: string | undefined;
}

export interface IMemorylaneRepository {
  refresh(): Promise<void>;
  cluster(userIds: string[], seed: number, limit: number): Promise<MemoryLaneCluster | undefined>;
  person(userIds: string[], seed: number, limit: number): Promise<MemoryLanePerson | undefined>;
  recentHighlight(userIds: string[], seed: number, limit: number): Promise<MemoryLaneRecentHighlights | undefined>;
  year(userIds: string[], seed: number, limit: number): Promise<MemoryLaneYear | undefined>;
}
