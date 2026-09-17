import { PrismaRepository } from '@api/repository/repository.service';
import { WAMonitoringService } from '@api/services/monitor.service';
import { Logger } from '@config/logger.config';
import axios from 'axios';

import { ChannelController, ChannelControllerInterface } from '../channel.controller';

export class MetaController extends ChannelController implements ChannelControllerInterface {
  private readonly logger = new Logger('MetaController');

  constructor(prismaRepository: PrismaRepository, waMonitor: WAMonitoringService) {
    super(prismaRepository, waMonitor);
  }

  integrationEnabled: boolean;

  public async receiveWebhook(data: any) {
    if (data?.object === 'whatsapp_business_account') {
      // Meta only needs its 200, and it must never wait on our downstream work: instance
      // lookups, chatbot fan-out, or the template branch axios.post to a customer URL that
      // carries no timeout. Blocking here risks Meta timing the endpoint out and disabling
      // it. processWebhookEntries contains every failure internally; the catch below is a
      // last-resort boundary so nothing can escape as an unhandled rejection.
      this.processWebhookEntries(data).catch((error) => {
        this.logger.error(`[META-WEBHOOK] unhandled failure: ${error?.message ?? error}`);
      });
    }

    return {
      status: 'success',
    };
  }

  // Every entry, and every change inside an entry, is routed and error-bounded on its own,
  // so one malformed or failing item can never discard the rest of a batched webhook.
  private async processWebhookEntries(data: any) {
    for (const entry of data?.entry ?? []) {
      try {
        for (const change of entry?.changes ?? []) {
          const field = change?.field;

          try {
            await this.processChange(data, entry, change);
          } catch (error) {
            this.logger.error(
              `[META-WEBHOOK] change failed field=${field} entry=${entry?.id} error=${error?.message ?? error}`,
            );
          }
        }
      } catch (error) {
        this.logger.error(`[META-WEBHOOK] entry failed entry=${entry?.id} error=${error?.message ?? error}`);
      }
    }
  }

  private async processChange(data: any, entry: any, change: any) {
    const field = change?.field;

    if (field === 'messages' || field === 'smb_message_echoes') {
      const numberId = change.value.metadata.phone_number_id;

      if (!numberId) {
        this.logger.error('WebhookService -> receiveWebhookMeta -> numberId not found');
        return {
          status: 'success',
        };
      }

      const instance = await this.prismaRepository.instance.findFirst({
        where: { number: numberId },
      });

      if (!instance) {
        this.logger.error('WebhookService -> receiveWebhookMeta -> instance not found');
        return {
          status: 'success',
        };
      }

      // Narrow to THIS entry and THIS change: whatsapp.business.service.ts:147 reads
      // data.entry[0].changes[0].value, so anything wider makes the resolved instance
      // process a different entry (a different tenant) or a different change.
      await this.waMonitor.waInstances[instance.name].connectToWhatsapp({
        ...data,
        entry: [{ ...entry, changes: [change] }],
      });

      return {
        status: 'success',
      };
    }

    if (field === 'message_template_status_update') {
      const templateId = `${change.value.message_template_id}`;

      const template = await this.prismaRepository.template.findFirst({
        where: { templateId },
      });

      if (!template) {
        this.logger.warn(`WebhookService -> receiveWebhookMeta -> template not found: ${templateId}`);
        return {
          status: 'success',
        };
      }

      const { webhookUrl } = template;

      await axios.post(webhookUrl, change.value, {
        headers: {
          'Content-Type': 'application/json',
        },
        timeout: 30000,
      });

      return {
        status: 'success',
      };
    }

    if (field === 'account_update') {
      const wabaId = entry.id;

      if (!wabaId) {
        this.logger.error('WebhookService -> receiveWebhookMeta -> account_update missing entry.id (WABA ID)');
        return {
          status: 'success',
        };
      }

      const instance = await this.prismaRepository.instance.findFirst({
        where: { businessId: wabaId },
      });

      if (!instance) {
        this.logger.error(`WebhookService -> receiveWebhookMeta -> instance not found for businessId ${wabaId}`);
        return {
          status: 'success',
        };
      }

      await this.waMonitor.waInstances[instance.name].accountUpdateHandler(wabaId, change.value);

      return {
        status: 'success',
      };
    }

    if (field === 'phone_number_quality_update') {
      const numberId = change.value?.phone_number_id;

      if (!numberId) {
        this.logger.error(
          'WebhookService -> receiveWebhookMeta -> phone_number_quality_update missing phone_number_id',
        );
        return {
          status: 'success',
        };
      }

      const instance = await this.prismaRepository.instance.findFirst({
        where: { number: numberId },
      });

      if (!instance) {
        this.logger.warn(
          `WebhookService -> receiveWebhookMeta -> instance not found for phone_number_id ${numberId} (field=phone_number_quality_update)`,
        );
        return {
          status: 'success',
        };
      }

      await this.waMonitor.waInstances[instance.name].phoneNumberQualityUpdateHandler(entry.id, change.value);

      return {
        status: 'success',
      };
    }

    if (field === 'account_alerts') {
      const wabaId = entry.id;

      if (!wabaId) {
        this.logger.error('WebhookService -> receiveWebhookMeta -> account_alerts missing entry.id (WABA ID)');
        return {
          status: 'success',
        };
      }

      const instance = await this.prismaRepository.instance.findFirst({
        where: { businessId: wabaId },
      });

      if (!instance) {
        this.logger.warn(
          `WebhookService -> receiveWebhookMeta -> instance not found for businessId ${wabaId} (field=account_alerts)`,
        );
        return {
          status: 'success',
        };
      }

      await this.waMonitor.waInstances[instance.name].accountAlertsHandler(wabaId, change.value);

      return {
        status: 'success',
      };
    }

    if (field === 'message_template_quality_update') {
      const wabaId = entry.id;

      if (!wabaId) {
        this.logger.error(
          'WebhookService -> receiveWebhookMeta -> message_template_quality_update missing entry.id (WABA ID)',
        );
        return {
          status: 'success',
        };
      }

      const instance = await this.prismaRepository.instance.findFirst({
        where: { businessId: wabaId },
      });

      if (!instance) {
        this.logger.warn(
          `WebhookService -> receiveWebhookMeta -> instance not found for businessId ${wabaId} (field=message_template_quality_update)`,
        );
        return {
          status: 'success',
        };
      }

      await this.waMonitor.waInstances[instance.name].messageTemplateQualityUpdateHandler(wabaId, change.value);

      return {
        status: 'success',
      };
    }

    this.logger.warn(
      `WebhookService -> receiveWebhookMeta -> unhandled field: ${field}, entry: ${entry?.id}, payload: ${JSON.stringify(change)}`,
    );

    return {
      status: 'success',
    };
  }
}
