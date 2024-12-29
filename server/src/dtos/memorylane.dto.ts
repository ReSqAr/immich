import { ApiProperty } from '@nestjs/swagger';
import { Type } from 'class-transformer';
import { IsInt, IsNotEmpty, IsPositive, IsString } from 'class-validator';
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

export class MemorylaneResponseDto {
  id!: string;
  @ApiProperty({ enumName: 'MemorylaneType', enum: MemorylaneType })
  type!: MemorylaneType;
  title!: string;
  parameter!: number;
  assets!: AssetResponseDto[];
}
