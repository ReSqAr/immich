import { ApiExtraModels, ApiProperty, getSchemaPath } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsArray, IsInt, IsNotEmpty, IsPositive, IsString, ValidateNested } from 'class-validator';
import { AssetResponseDto } from 'src/dtos/asset-response.dto';
import { MemorylaneType } from 'src/enum';
import { Optional } from 'src/validation';

export class MemorylaneParamDto {
  @IsNotEmpty()
  @IsString()
  id!: string;
}

export class MemorylaneQueryDto {
  @Optional()
  @IsInt()
  @IsPositive()
  @Type(() => Number)
  limit?: number;
  @ApiProperty({ enumName: 'MemorylaneType', enum: MemorylaneType })
  @Optional()
  type?: MemorylaneType;
}

export class MemorylaneRequest {
  @IsNotEmpty()
  @IsString()
  id!: string;

  @Optional()
  @IsInt()
  @IsPositive()
  @Type(() => Number)
  limit?: number;

  @ApiProperty({ enumName: 'MemorylaneType', enum: MemorylaneType })
  @Optional()
  type?: MemorylaneType;
}

export class MemorylanesBodyDto {
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => MemorylaneRequest)
  requests!: MemorylaneRequest[];
}

export class MemorlaneClusterMetadata {
  clusterID?: number;
  startDate?: Date;
  endDate?: Date;
  locations?: string[];
}

export class MemorlanePersonMetadata {
  personName?: string;
}

export class MemorlaneRecentHighlightsMetadata {}

export class MemorlaneSimilarityMetadata {
  category?: string;
}

export class MemorlaneYearMetadata {
  year?: string;
}

@ApiExtraModels(
  MemorlaneClusterMetadata,
  MemorlanePersonMetadata,
  MemorlaneRecentHighlightsMetadata,
  MemorlaneSimilarityMetadata,
  MemorlaneYearMetadata,
)
export class MemorylaneResponseDto {
  id!: string;
  @ApiProperty({ enumName: 'MemorylaneType', enum: MemorylaneType })
  type!: MemorylaneType;
  assets!: AssetResponseDto[];

  @ApiProperty({
    oneOf: [
      { $ref: getSchemaPath(MemorlaneClusterMetadata) },
      { $ref: getSchemaPath(MemorlanePersonMetadata) },
      { $ref: getSchemaPath(MemorlaneRecentHighlightsMetadata) },
      { $ref: getSchemaPath(MemorlaneSimilarityMetadata) },
      { $ref: getSchemaPath(MemorlaneYearMetadata) },
    ],
    discriminator: {
      propertyName: 'type',
      mapping: {
        [MemorylaneType.CLUSTER]: getSchemaPath(MemorlaneClusterMetadata),
        [MemorylaneType.PERSON]: getSchemaPath(MemorlanePersonMetadata),
        [MemorylaneType.RECENT_HIGHLIGHTS]: getSchemaPath(MemorlaneRecentHighlightsMetadata),
        [MemorylaneType.SIMILARITY]: getSchemaPath(MemorlaneSimilarityMetadata),
        [MemorylaneType.YEAR]: getSchemaPath(MemorlaneYearMetadata),
      },
    },
  })
  metadata!:
    | MemorlaneClusterMetadata
    | MemorlanePersonMetadata
    | MemorlaneRecentHighlightsMetadata
    | MemorlaneSimilarityMetadata
    | MemorlaneYearMetadata;
}
