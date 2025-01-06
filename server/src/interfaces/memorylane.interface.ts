export const IMemorylaneRepository = 'IMemorylaneRepository';

interface Memorylane {
  assetIds: string[];
}

interface Memorylane {
  assetIds: string[];
}

type RecentHighlightsMemoryLane = Memorylane;

interface SpotlightYearMemoryLane extends Memorylane {
  year: string | undefined;
}

interface ClusterMemoryLane extends Memorylane {
  startDate: Date | undefined;
  endDate: Date | undefined;
  locations: string[];
}

interface PersonMemoryLane extends Memorylane {
  personName: string | undefined;
}

export interface IMemorylaneRepository {
  refresh(): Promise<void>;
  recentHighlight(userIds: string[], seed: number, limit: number): Promise<RecentHighlightsMemoryLane>;
  cluster(userIds: string[], seed: number, limit: number): Promise<ClusterMemoryLane>;
  person(userIds: string[], seed: number, limit: number): Promise<PersonMemoryLane>;
  year(userIds: string[], seed: number, limit: number): Promise<SpotlightYearMemoryLane>;
}
