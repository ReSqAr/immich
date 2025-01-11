import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { AssetEntity } from 'src/entities/asset.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import {
  IMemorylaneRepository,
  MemoryLaneCluster,
  MemoryLanePerson,
  MemoryLaneRecentHighlights,
  MemoryLaneYear,
} from 'src/interfaces/memorylane.interface';
import {
  memorylaneClusterQuery,
  memorylanePersonQuery,
  memorylaneRecentHighlightsQuery,
  memorylaneYearQuery,
} from 'src/resources/sql';
import { Repository } from 'typeorm';

type LocationStatistics = {
  cities?: Record<string, number>;
  states?: Record<string, number>;
  countries?: Record<string, number>;
};

function isLocationScattered(locations: Record<string, number>, threshold = 0.05, topN = 2): boolean {
  if (!locations || Object.keys(locations).length === 0) {
    return true;
  }

  const frequencies = Object.entries(locations)
    .filter(([location]) => location !== 'unknown')
    .map(([, freq]) => freq)
    .sort((a, b) => b - a);

  if (frequencies.length === 0) {
    return true;
  }

  const remainder = frequencies.slice(topN).reduce((sum, freq) => sum + freq, 0);
  return remainder > threshold;
}

function getTopLocations(locations: Record<string, number>, threshold = 0.05, topN = 2): string[] {
  return Object.entries(locations)
    .filter(([location, freq]) => location && location.trim() && location !== 'unknown' && freq >= threshold)
    .sort(([, freqA], [, freqB]) => freqB - freqA)
    .slice(0, topN)
    .map(([location]) => location);
}

function extractLocations(locationStats: LocationStatistics): string[] {
  if (locationStats?.cities && !isLocationScattered(locationStats.cities)) {
    const topCities = getTopLocations(locationStats.cities);
    if (topCities.length > 0) {
      return topCities;
    }
  } else if (locationStats?.states && !isLocationScattered(locationStats.states)) {
    const topStates = getTopLocations(locationStats.states);
    if (topStates.length > 0) {
      return topStates;
    }
  } else if (locationStats?.countries && !isLocationScattered(locationStats.countries, 0.05, 10)) {
    const topCountries = getTopLocations(locationStats.countries, 0.05, 10);
    if (topCountries.length > 0) {
      return topCountries;
    }
  }

  return [];
}

@Injectable()
export class MemorylaneRepository implements IMemorylaneRepository {
  constructor(
    @InjectRepository(AssetEntity) private assetRepository: Repository<AssetEntity>,
    @Inject(ILoggerRepository) private logger: ILoggerRepository,
  ) {
    this.logger.setContext(MemorylaneRepository.name);
  }

  async refresh(): Promise<void> {
    const startTime = Date.now();

    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_dbscan');
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_dbscan_clusters');
    await this.assetRepository.query('REFRESH MATERIALIZED VIEW asset_analysis');

    const duration = Date.now() - startTime;
    this.logger.debug(`refreshed all memorylane materialized views in ${duration}ms`);
  }

  async cluster(userIds: string[], seed: number, limit: number): Promise<MemoryLaneCluster | undefined> {
    const result = await this.assetRepository.query(memorylaneClusterQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    if (!result || result.length === 0) {
      return undefined;
    }

    const {
      cluster_id: clusterID,
      cluster_start: startDate,
      cluster_end: endDate,
      cluster_location_distribution: clusterLocationDistribution,
    } = result[0];

    return {
      clusterID,
      locations: extractLocations(clusterLocationDistribution),
      startDate: new Date(startDate),
      endDate: new Date(endDate),
      assetIds,
    };
  }

  async person(userIds: string[], seed: number, limit: number): Promise<MemoryLanePerson | undefined> {
    const result = await this.assetRepository.query(memorylanePersonQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    if (!result || result.length === 0) {
      return undefined;
    }

    const { person_name: personName } = result[0];
    return { personName, assetIds };
  }

  async recentHighlight(
    userIds: string[],
    seed: number,
    limit: number,
  ): Promise<MemoryLaneRecentHighlights | undefined> {
    const result = await this.assetRepository.query(memorylaneRecentHighlightsQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    if (!result || result.length === 0) {
      return undefined;
    }

    return { assetIds };
  }

  async year(userIds: string[], seed: number, limit: number): Promise<MemoryLaneYear | undefined> {
    const result = await this.assetRepository.query(memorylaneYearQuery, [seed, limit, userIds]);
    const assetIds: string[] = result.map(({ id }: { id: string }) => id);

    if (!result || result.length === 0) {
      return undefined;
    }

    const { year } = result[0];
    return { year: `${year}`, assetIds };
  }
}
