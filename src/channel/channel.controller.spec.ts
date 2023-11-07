import { Test, TestingModule } from '@nestjs/testing';
import { ChannelController } from './channel.controller';

describe('ChannelController', () => {
  let controller: ChannelController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [ChannelController],
    }).compile();

    controller = module.get<ChannelController>(ChannelController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });

  it('should return the channel id', () => {
    const channelId = 'abc';
    expect(controller.getChannelInfo(channelId)).toBe(channelId);
  });
});
