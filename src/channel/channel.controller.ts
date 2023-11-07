import { Controller, Get, Param, Req } from '@nestjs/common';

@Controller('channel')
export class ChannelController {
    @Get(':id')
    getChannelInfo(@Param('id') channelId: string){
        return channelId;
    }
}
