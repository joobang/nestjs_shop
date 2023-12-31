import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { SearchController } from './search/search.controller';
import { SearchService } from './search/search.service';
import { SearchModule } from './search/search.module';
import { ChannelController } from './channel/channel.controller';
import { ChannelService } from './channel/channel.service';
import { ChannelModule } from './channel/channel.module';
import { AuthModule } from './auth/auth.module';
import { UsersModule } from './users/users.module';

@Module({
  imports: [SearchModule, ChannelModule, AuthModule, UsersModule],
  controllers: [AppController, SearchController, ChannelController],
  providers: [AppService, SearchService, ChannelService],
})
export class AppModule {}
