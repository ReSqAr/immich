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
  cluster(userIds: string[], seed: number, limit: number): Promise<MemoryLaneCluster>;
  person(userIds: string[], seed: number, limit: number): Promise<MemoryLanePerson>;
  recentHighlight(userIds: string[], seed: number, limit: number): Promise<MemoryLaneRecentHighlights>;
  year(userIds: string[], seed: number, limit: number): Promise<MemoryLaneYear>;
}
