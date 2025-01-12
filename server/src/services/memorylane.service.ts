import { Injectable } from '@nestjs/common';
import { OnJob } from 'src/decorators';
import { mapAsset } from 'src/dtos/asset-response.dto';
import { AuthDto } from 'src/dtos/auth.dto';
import {
  MemorlaneClusterMetadata,
  MemorlanePersonMetadata,
  MemorlaneRecentHighlightsMetadata,
  MemorlaneSimilarityMetadata,
  MemorlaneThisDayMetadata,
  MemorlaneYearMetadata,
  MemorylaneResponseDto,
} from 'src/dtos/memorylane.dto';
import { AssetEntity } from 'src/entities/asset.entity';
import { MemorylaneType } from 'src/enum';
import { JobName, JobOf, JobStatus, QueueName } from 'src/interfaces/job.interface';
import { BaseService } from 'src/services/base.service';
import { getMyPartnerIds } from 'src/utils/asset.util';
import { isSmartSearchEnabled } from 'src/utils/misc';

const DEFAULT_LIMIT = 12;
const BINS = 1367;
const HISTOGRAM_BUCKETS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000];

const CLIP_QUERIES = {
  nature: [
    { query: 'a beautiful sunset', weight: 1 },
    { query: 'morning sunrise', weight: 1 },
    { query: 'scenic mountain landscape', weight: 1 },
    { query: 'peaceful beach scene', weight: 1 },
    { query: 'autumn leaves', weight: 0.8 },
    { query: 'flowers in bloom', weight: 0.8 },
    { query: 'forest path', weight: 0.7 },
    { query: 'rolling hills landscape', weight: 0.9 },
    { query: 'desert sand dunes', weight: 0.9 },
    { query: 'waterfall in nature', weight: 1 },
    { query: 'tropical paradise', weight: 0.9 },
    { query: 'spring cherry blossoms', weight: 0.9 },
    { query: 'pristine lake reflection', weight: 1 },
    { query: 'majestic mountains', weight: 1 },
  ],
  urban: [
    { query: 'city nightlife', weight: 1 },
    { query: 'urban architecture', weight: 0.9 },
    { query: 'street photography', weight: 0.8 },
    { query: 'city lights at night', weight: 1 },
    { query: 'modern skyscrapers', weight: 0.9 },
    { query: 'historic buildings', weight: 0.9 },
    { query: 'busy street scene', weight: 0.8 },
    { query: 'urban park life', weight: 0.8 },
    { query: 'city reflection puddle', weight: 0.9 },
    { query: 'neon city signs', weight: 1 },
    { query: 'geometric architecture', weight: 0.9 },
    { query: 'urban sunset skyline', weight: 1 },
  ],
  moments: [
    { query: 'happy celebration', weight: 1 },
    { query: 'family gathering', weight: 1 },
    { query: 'candid moment', weight: 0.9 },
    { query: 'group photo', weight: 0.8 },
    { query: 'birthday party', weight: 0.8 },
    { query: 'joyful laughter', weight: 1 },
    { query: 'emotional embrace', weight: 0.9 },
    { query: 'festive celebration', weight: 1 },
    { query: 'quiet contemplation', weight: 0.8 },
    { query: 'milestone celebration', weight: 0.9 },
    { query: 'surprise reaction', weight: 0.9 },
    { query: 'peaceful meditation', weight: 0.8 },
  ],
  activities: [
    { query: 'outdoor adventure', weight: 1 },
    { query: 'sports action', weight: 0.9 },
    { query: 'hiking trail', weight: 0.8 },
    { query: 'beach vacation', weight: 1 },
    { query: 'travel photography', weight: 1 },
    { query: 'rock climbing action', weight: 0.9 },
    { query: 'cycling adventure', weight: 0.9 },
    { query: 'kayaking expedition', weight: 0.9 },
    { query: 'camping in nature', weight: 0.9 },
    { query: 'skiing action shot', weight: 1 },
    { query: 'surfing waves', weight: 1 },
    { query: 'yoga practice', weight: 0.8 },
  ],
  weather: [
    { query: 'snowy landscape', weight: 1 },
    { query: 'rainy day', weight: 0.8 },
    { query: 'foggy morning', weight: 0.9 },
    { query: 'stormy sky', weight: 0.8 },
    { query: 'misty mountains', weight: 1 },
    { query: 'dramatic clouds', weight: 1 },
    { query: 'rainbow after rain', weight: 1 },
    { query: 'golden hour glow', weight: 1 },
    { query: 'moody weather', weight: 0.9 },
    { query: 'frost covered scene', weight: 0.9 },
    { query: 'lightning strike', weight: 0.8 },
  ],
  composition: [
    { query: 'dramatic lighting', weight: 1 },
    { query: 'silhouette photo', weight: 0.9 },
    { query: 'reflection in water', weight: 1 },
    { query: 'aerial view', weight: 0.8 },
    { query: 'close-up detail', weight: 0.7 },
    { query: 'leading lines', weight: 0.9 },
    { query: 'minimalist scene', weight: 0.9 },
    { query: 'symmetrical composition', weight: 1 },
    { query: 'bokeh background', weight: 0.9 },
    { query: 'framed through window', weight: 0.8 },
    { query: 'long exposure shot', weight: 0.9 },
    { query: 'panoramic vista', weight: 1 },
  ],
  pets: [
    { query: 'playful pet', weight: 1 },
    { query: 'sleeping cat', weight: 0.9 },
    { query: 'dog portrait', weight: 1 },
    { query: 'pet and owner moment', weight: 1 },
    { query: 'curious animal', weight: 0.9 },
    { query: 'pets playing together', weight: 0.9 },
    { query: 'animal close-up', weight: 0.8 },
    { query: 'funny pet expression', weight: 0.9 },
  ],
  food: [
    { query: 'delicious meal', weight: 1 },
    { query: 'colorful dessert', weight: 0.9 },
    { query: 'food presentation', weight: 0.9 },
    { query: 'cooking preparation', weight: 0.8 },
    { query: 'fresh ingredients', weight: 0.9 },
    { query: 'birthday cake', weight: 1 },
    { query: 'festive dinner', weight: 1 },
    { query: 'breakfast spread', weight: 0.9 },
  ],
  seasonal: [
    { query: 'winter wonderland', weight: 1 },
    { query: 'summer beach day', weight: 1 },
    { query: 'fall foliage colors', weight: 1 },
    { query: 'spring flowers bloom', weight: 1 },
    { query: 'christmas decoration', weight: 0.9 },
    { query: 'halloween celebration', weight: 0.9 },
    { query: 'new year fireworks', weight: 1 },
    { query: 'seasonal festival', weight: 0.9 },
  ],
};

const FLATTENED_QUERIES = Object.values(CLIP_QUERIES).flat();

async function stringToSignedSHA32(str: string): Promise<number> {
  const buffer = new TextEncoder().encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const view = new DataView(hashBuffer);

  return (
    view.getUint32(0) % 2 ** 31 ^
    view.getUint32(4) % 2 ** 31 ^
    view.getUint32(8) % 2 ** 31 ^
    view.getUint32(12) % 2 ** 31
  );
}

const MIN_TIME_BETWEEN_PHOTOS = 15 * 60 * 1000;

interface WeightedItem<T> {
  item: T;
  weight: number;
  leftCumulative: number;
  rightCumulative: number;
}

function computeCumulativeWeights<T>(items: T[], getWeight: (item: T) => number): WeightedItem<T>[] {
  let cumulative = 0;
  return items.map((item) => {
    const weight = getWeight(item);
    const leftCumulative = cumulative;
    cumulative += weight;
    return {
      item,
      weight,
      leftCumulative,
      rightCumulative: cumulative,
    };
  });
}

function findItemForRandom<T>(items: WeightedItem<T>[], random: number): T | undefined {
  const last = items.at(-1);
  if (last === undefined) {
    return undefined;
  }

  const scaledTotal = Math.round(last.rightCumulative * BINS);
  const targetValue = random % scaledTotal;

  return items.find((item) => item.leftCumulative * BINS <= targetValue && targetValue < item.rightCumulative * BINS)
    ?.item;
}

function capitalizeWords(str: string): string {
  return str
    .split(' ')
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function randomMemorylaneType(seed: number, memorylaneWeights: Record<MemorylaneType, number>) {
  const weights = Object.entries(memorylaneWeights).map(([type, weight]) => ({ item: type as MemorylaneType, weight }));
  const weightedTypes = computeCumulativeWeights(weights, (item) => item.weight);
  return findItemForRandom(weightedTypes, seed)?.item || MemorylaneType.RECENT_HIGHLIGHTS;
}

export function selectRandomQuery(seed: number): string {
  const weightedQueries = computeCumulativeWeights(FLATTENED_QUERIES, (item) => item.weight);
  const selected = findItemForRandom(weightedQueries, seed);
  return selected?.query ?? 'beautiful photo';
}

/*
  This function is inspired by:
    https://stackoverflow.com/questions/664014/what-integer-hash-function-are-good-that-accepts-an-integer-hash-key
  In tests this is good enough.
 */
function getRandom(seed: number, i: number) {
  const CONST: bigint = 73_244_475n;
  const result: bigint = (BigInt(seed ^ i) * CONST) % 4_294_967_296n; // 2^32
  return Number(result);
}

export function selectRandomPhotos(assets: AssetEntity[], seed: number, limit: number): AssetEntity[] {
  const weightedAssets = computeCumulativeWeights(assets, () => 1);

  const selected: AssetEntity[] = [];

  for (let i = 0; i < limit * 4 && selected.length < limit; i++) {
    const random = getRandom(seed, i);

    const candidate = findItemForRandom(weightedAssets, random);
    if (!candidate) {
      continue;
    }

    const isTooClose = selected.some((selectedPhoto) => {
      const timeDiff = Math.abs(candidate.localDateTime.getTime() - selectedPhoto.localDateTime.getTime());
      return timeDiff < MIN_TIME_BETWEEN_PHOTOS;
    });

    if (!isTooClose) {
      selected.push(candidate);
    }
  }

  return selected;
}

@Injectable()
export class MemorylaneService extends BaseService {
  private async loadAssetIds(assetIds: string[]) {
    const assets = await this.assetRepository.getByIds(assetIds, { exifInfo: true, qualityAssessment: true });

    const assetMap = new Map(assets.map((asset) => [asset.id, asset]));

    return assetIds
      .map((id) => assetMap.get(id))
      .filter((asset) => asset !== undefined)
      .map((asset) => mapAsset(asset));
  }

  private async getUserIdsToSearch(auth: AuthDto): Promise<string[]> {
    const partnerIds = await getMyPartnerIds({
      userId: auth.user.id,
      repository: this.partnerRepository,
      timelineEnabled: true,
    });
    return [auth.user.id, ...partnerIds];
  }

  private async similarity(userIds: string[], seed: number, limit: number) {
    const query = selectRandomQuery(seed);
    const pagination = { page: 1, size: 4 * limit };
    const { machineLearning } = await this.getConfig({ withCache: false });
    if (!isSmartSearchEnabled(machineLearning)) {
      return;
    }

    const embedding = await this.machineLearningRepository.encodeText(
      machineLearning.urls,
      query,
      machineLearning.clip,
    );

    const { mean } = await this.qualityAssessmentRepository.scoreDistribution(userIds);
    const minimumScore = mean;
    const options = { userIds, embedding, withQualityAssessment: true, withExif: true, minimumScore };
    const result = await this.searchRepository.searchSmart(pagination, options);
    const selectedAssets = selectRandomPhotos(result.items, seed, limit);

    if (!selectedAssets || selectedAssets.length === 0) {
      return;
    }

    return {
      query,
      assets: selectedAssets.map((asset) => mapAsset(asset)),
    };
  }

  async get(
    auth: AuthDto,
    memorylane: MemorylaneType | undefined,
    id: string,
    limit: number | undefined,
  ): Promise<MemorylaneResponseDto | undefined> {
    // TODO
    //await this.requireAccess({auth, permission: Permission.MEMORY_READ, ids: [id]});

    const start = Date.now();

    const seed = await stringToSignedSHA32(id);
    const effectiveLimit = limit || DEFAULT_LIMIT;

    const memorylaneWeights = await this.getConfig({ withCache: false }).then(
      (value) => value['memorylane']['weights'],
    );
    const effectiveMemorylane = memorylane || randomMemorylaneType(seed, memorylaneWeights);

    const userIds = await this.getUserIdsToSearch(auth);

    let result;
    switch (effectiveMemorylane) {
      case MemorylaneType.CLUSTER: {
        const clusterResult = await this.memorylaneRepository.cluster(userIds, seed, effectiveLimit);
        if (!clusterResult) {
          this.logger.debug(`Could not find assets for memorylane ${effectiveMemorylane}`)
          return undefined;
        }

        const { assetIds, clusterID, locations, startDate, endDate } = clusterResult;
        result = {
          id,
          type: MemorylaneType.CLUSTER,
          metadata: { clusterID, locations, startDate, endDate } as MemorlaneClusterMetadata,
          assets: await this.loadAssetIds(assetIds),
        };
        break;
      }
      case MemorylaneType.PERSON: {
        const personResult = await this.memorylaneRepository.person(userIds, seed, effectiveLimit);
        if (!personResult) {
          this.logger.debug(`Could not find assets for memorylane ${effectiveMemorylane}`)
          return undefined;
        }

        const { assetIds, personName, personID } = personResult;
        result = {
          id,
          type: MemorylaneType.PERSON,
          metadata: { personName, personID } as MemorlanePersonMetadata,
          assets: await this.loadAssetIds(assetIds),
        };
        break;
      }
      case MemorylaneType.RECENT_HIGHLIGHTS: {
        const recentHighlightsResult = await this.memorylaneRepository.recentHighlight(userIds, seed, effectiveLimit);
        if (!recentHighlightsResult) {
          this.logger.debug(`Could not find assets for memorylane ${effectiveMemorylane}`)
          return undefined;
        }

        const { assetIds } = recentHighlightsResult;
        result = {
          id,
          type: MemorylaneType.RECENT_HIGHLIGHTS,
          metadata: {} as MemorlaneRecentHighlightsMetadata,
          assets: await this.loadAssetIds(assetIds),
        };
        break;
      }
      case MemorylaneType.THIS_DAY: {
        const thisDayResult = await this.memorylaneRepository.thisDay(userIds, seed, effectiveLimit);
        if (!thisDayResult) {
          this.logger.debug(`Could not find assets for memorylane ${effectiveMemorylane}`)
          return undefined;
        }

        const { assetIds, nYearsAgo, year } = thisDayResult;
        result = {
          id,
          type: MemorylaneType.THIS_DAY,
          metadata: { nYearsAgo, year } as MemorlaneThisDayMetadata,
          assets: await this.loadAssetIds(assetIds),
        };
        break;
      }
      case MemorylaneType.SIMILARITY: {
        const similarityResult = await this.similarity(userIds, seed, effectiveLimit);
        if (!similarityResult) {
          this.logger.debug(`Could not find assets for memorylane ${effectiveMemorylane}`)
          return undefined;
        }

        const { assets, query } = similarityResult;
        result = {
          id,
          type: MemorylaneType.SIMILARITY,
          metadata: { category: capitalizeWords(query) } as MemorlaneSimilarityMetadata,
          assets,
        };
        break;
      }
      case MemorylaneType.YEAR: {
        const yearResult = await this.memorylaneRepository.year(userIds, seed, effectiveLimit);
        if (!yearResult) {
          this.logger.debug(`Could not find assets for memorylane ${effectiveMemorylane}`)
          return undefined;
        }

        const { assetIds, year } = yearResult;
        result = {
          id,
          type: MemorylaneType.YEAR,
          metadata: { year } as MemorlaneYearMetadata,
          assets: await this.loadAssetIds(assetIds),
        };
        break;
      }
    }

    this.telemetryRepository.api.addToHistogram(`memorylane.${effectiveMemorylane}.load`, Date.now() - start, {
      description: 'Duration of memorylane load operation',
      unit: 'ms',
      advice: {
        explicitBucketBoundaries: HISTOGRAM_BUCKETS,
      },
    });
    return result;
  }

  @OnJob({ name: JobName.MEMORYLANE_REFRESH, queue: QueueName.BACKGROUND_TASK })
  async handleRefreshMemorylane({}: JobOf<JobName.QUEUE_IQA_SCORE>): Promise<JobStatus> {
    const start = Date.now();

    await this.memorylaneRepository.refresh();

    this.telemetryRepository.api.addToHistogram(`memorylane.materialised_views.refresh`, Date.now() - start, {
      description: 'Duration of memorylane materialise views refresh',
      unit: 'ms',
      advice: {
        explicitBucketBoundaries: HISTOGRAM_BUCKETS,
      },
    });

    return JobStatus.SUCCESS;
  }
}
