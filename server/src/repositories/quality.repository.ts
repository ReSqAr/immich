import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { AssetEntity } from 'src/entities/asset.entity';
import { QualityEntity } from 'src/entities/quality.entity';
import { IQualityRepository } from 'src/interfaces/quality.interface';
import { Repository } from 'typeorm';

@Injectable()
export class QualityRepository implements IQualityRepository {
  constructor(
    @InjectRepository(QualityEntity) private qualityRepository: Repository<QualityEntity>,
    @InjectRepository(AssetEntity) private assetRepository: Repository<AssetEntity>,
  ) {}

  async upsert(assetId: string, score: number): Promise<void> {
    await this.qualityRepository.upsert({ assetId, score }, { conflictPaths: ['assetId'] });
  }

  async clearAllIQAScores(): Promise<void> {
    return this.qualityRepository.clear();
  }
}
