import { BadRequestException, Injectable } from '@nestjs/common';
import { OnJob } from 'src/decorators';
import { mapAsset } from 'src/dtos/asset-response.dto';
import { AuthDto } from 'src/dtos/auth.dto';
import { MemorylaneResponseDto } from 'src/dtos/memorylane.dto';
import { AssetEntity } from 'src/entities/asset.entity';
import { MemorylaneType } from 'src/enum';
import { JobName, JobOf, JobStatus, QueueName } from 'src/interfaces/job.interface';
import { BaseService } from 'src/services/base.service';
import { getMyPartnerIds } from 'src/utils/asset.util';
import { isSmartSearchEnabled } from 'src/utils/misc';

const LIMIT = 12;
const SALT = 'hunter2!';

const MEMORYLANE_WEIGHTS = [
  { item: MemorylaneType.RECENT_HIGHLIGHTS, weight: 0.1 },
  { item: MemorylaneType.CLUSTER, weight: 0.7 },
  { item: MemorylaneType.SIMILARITY, weight: 0.2 },
];

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

async function stringToSignedSHA32(str: string, salt: string): Promise<number> {
  const buffer = new TextEncoder().encode(salt + str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
  const view = new DataView(hashBuffer);
  return view.getInt32(0);
}

const MIN_TIME_BETWEEN_PHOTOS = 15 * 60 * 1000; // 15 minutes in milliseconds
const LCG_M = 2_147_483_648; // 2^31

function lcg(seed: number): number {
  // Ensure seed is positive and within bounds
  const normalizedSeed = (seed + LCG_M) % LCG_M;
  // Use same constants as SQL implementation
  return (1_103_515_245 * normalizedSeed + 12_345) % LCG_M;
}

function lcgToFloat(seed: number): number {
  return seed / LCG_M;
}

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
  const targetValue = random * last.rightCumulative;
  return items.find((item) => item.leftCumulative <= targetValue && targetValue < item.rightCumulative)?.item;
}

function randomMemorylaneType(seed: number) {
  const weightedTypes = computeCumulativeWeights(MEMORYLANE_WEIGHTS, (item) => item.weight);

  const random = lcgToFloat(lcg(seed));
  return findItemForRandom(weightedTypes, random)?.item || MemorylaneType.RECENT_HIGHLIGHTS;
}

export function selectRandomQuery(seed: number): string {
  const weightedQueries = computeCumulativeWeights(FLATTENED_QUERIES, (item) => item.weight);

  const random = lcgToFloat(lcg(seed));
  const selected = findItemForRandom(weightedQueries, random);
  console.log(
    `random=${random} selected=${JSON.stringify(selected)} weightedQueries=${JSON.stringify(weightedQueries)}`,
  );

  return selected?.query ?? 'beautiful photo';
}

export function selectRandomPhotos(assets: AssetEntity[], seed: number, limit: number): AssetEntity[] {
  // Compute weights (using equal weights for now, but could be modified to use quality scores)
  const weightedAssets = computeCumulativeWeights(assets, () => 1);

  const selected: AssetEntity[] = [];
  let currentSeed = seed;

  // Generate 2 * limit candidates to ensure we have enough after filtering
  for (let i = 0; i < limit * 4 && selected.length < limit; i++) {
    // Generate next random number
    currentSeed = lcg(currentSeed);
    const random = lcgToFloat(currentSeed);

    // Select a candidate
    const candidate = findItemForRandom(weightedAssets, random);
    if (!candidate) {
      continue;
    }

    // Check time spacing with already selected photos
    const isTooClose = selected.some((selectedPhoto) => {
      const timeDiff = Math.abs(candidate.localDateTime.getTime() - selectedPhoto.localDateTime.getTime());
      return timeDiff < MIN_TIME_BETWEEN_PHOTOS;
    });

    if (!isTooClose) {
      selected.push(candidate);
    }
  }

  // Sort final selection by time
  return selected;
}

@Injectable()
export class MemorylaneService extends BaseService {
  private async loadAssetIds(assetIds: string[], id: string, memorylane: MemorylaneType, title: string, seed: number) {
    const assets = await this.assetRepository.getByIds(assetIds, { exifInfo: true, qualityScore: true });

    // Create a map of assets by their IDs for efficient lookup
    const assetMap = new Map(assets.map((asset) => [asset.id, asset]));

    // Map the assets in the original order using result.assetIds
    const orderedAssets = assetIds
      .map((id) => assetMap.get(id))
      .filter((asset) => asset !== undefined)
      .map((asset) => mapAsset(asset));

    return {
      id,
      type: memorylane,
      title,
      parameter: seed,
      assets: orderedAssets,
    };
  }

  private async getUserIdsToSearch(auth: AuthDto): Promise<string[]> {
    const partnerIds = await getMyPartnerIds({
      userId: auth.user.id,
      repository: this.partnerRepository,
      timelineEnabled: true,
    });
    return [auth.user.id, ...partnerIds];
  }

  private async similarity(userIds: string[], id: string, seed: number, limit: number) {
    const query = selectRandomQuery(seed);
    const pagination = { page: 1, size: 4 * limit };
    const { machineLearning } = await this.getConfig({ withCache: false });
    if (!isSmartSearchEnabled(machineLearning)) {
      throw new BadRequestException('Smart search is not enabled');
    }
    const embedding = await this.machineLearningRepository.encodeText(
      machineLearning.urls,
      query,
      machineLearning.clip,
    );
    const options = { userIds, embedding, withQualityAssessment: true, withExif: true };
    const result = await this.searchRepository.searchSmart(pagination, options);
    const selectedAssets = selectRandomPhotos(result.items, seed, limit);

    return {
      id,
      type: MemorylaneType.SIMILARITY,
      title: query,
      parameter: seed,
      assets: selectedAssets.map((asset) => mapAsset(asset)),
    };
  }

  async get(
    auth: AuthDto,
    memorylane: MemorylaneType | undefined,
    id: string,
    limit: number | undefined,
  ): Promise<MemorylaneResponseDto> {
    //??
    //await this.requireAccess({auth, permission: Permission.MEMORY_READ, ids: [id]});

    const seed = await stringToSignedSHA32(id, SALT);

    const effectiveLimit = limit || LIMIT;
    const effectiveMemorylane = memorylane || randomMemorylaneType(seed);

    const userIds = await this.getUserIdsToSearch(auth);

    switch (effectiveMemorylane) {
      case MemorylaneType.CLUSTER: {
        const { assetIds, title } = await this.memorylaneRepository.cluster(userIds, seed, effectiveLimit);
        return await this.loadAssetIds(assetIds, id, MemorylaneType.CLUSTER, title, seed);
      }
      case MemorylaneType.RECENT_HIGHLIGHTS: {
        const { assetIds, title } = await this.memorylaneRepository.recentHighlight(userIds, seed, effectiveLimit);
        return await this.loadAssetIds(assetIds, id, MemorylaneType.RECENT_HIGHLIGHTS, title, seed);
      }
      case MemorylaneType.SIMILARITY: {
        return await this.similarity(userIds, id, seed, effectiveLimit);
      }
    }
  }

  @OnJob({ name: JobName.MEMORYLANE_REFRESH, queue: QueueName.BACKGROUND_TASK })
  async handleRefershMemorylane({}: JobOf<JobName.QUEUE_IQA_SCORE_GENERATION>): Promise<JobStatus> {
    await this.memorylaneRepository.refresh();
    return JobStatus.SUCCESS;
  }
}
