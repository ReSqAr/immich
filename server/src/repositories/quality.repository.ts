import { Inject, Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { QualityEntity } from 'src/entities/quality.entity';
import { ILoggerRepository } from 'src/interfaces/logger.interface';
import { IQualityRepository } from 'src/interfaces/quality.interface';
import { Repository } from 'typeorm';

@Injectable()
export class QualityRepository implements IQualityRepository {
  constructor(
    @Inject(ILoggerRepository) protected logger: ILoggerRepository,
    @InjectRepository(QualityEntity) private qualityRepository: Repository<QualityEntity>,
  ) {}

  async upsert(assetId: string, score: number): Promise<void> {
    await this.qualityRepository.upsert({ assetId, score }, { conflictPaths: ['assetId'] });
  }

  async clearAllIQAScores(): Promise<void> {
    return this.qualityRepository.clear();
  }
}
