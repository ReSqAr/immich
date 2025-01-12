export const IMemorylaneRepository = 'IMemorylaneRepository';

interface Memorylane {
  assetIds: string[];
}

export interface MemoryLaneCluster extends Memorylane {
  clusterID: number;
  startDate: Date;
  endDate: Date;
  locations: string[];
}

export interface MemoryLanePerson extends Memorylane {
  personID: string;
  personName: string | undefined;
}
export type MemoryLaneRecentHighlights = Memorylane;

export interface MemoryLaneThisDay extends Memorylane {
  year: string;
  nYearsAgo: string;
}

export interface MemoryLaneYear extends Memorylane {
  year: string;
}

export interface IMemorylaneRepository {
  refresh(): Promise<void>;
  cluster(userIds: string[], seed: number, limit: number): Promise<MemoryLaneCluster | undefined>;
  person(userIds: string[], seed: number, limit: number): Promise<MemoryLanePerson | undefined>;
  recentHighlight(userIds: string[], seed: number, limit: number): Promise<MemoryLaneRecentHighlights | undefined>;
  thisDay(userIds: string[], seed: number, limit: number): Promise<MemoryLaneThisDay | undefined>;
  year(userIds: string[], seed: number, limit: number): Promise<MemoryLaneYear | undefined>;
}
