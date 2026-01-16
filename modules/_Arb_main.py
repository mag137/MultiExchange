async with (ExchangeInstance(ccxt, cls.exchange_id, log=True) as cls.exchange1):
    success = await sync_time_with_exchange(cls.exchange1)
    if not success:
        cprint.error_b("Не удалось синхронизировать время с биржей.")
        return
    async with (ExchangeInstance(ccxt, cls.exchange_id, log=True) as cls.exchange2):
        success = await sync_time_with_exchange(cls.exchange2)
        if not success:
            cprint.error_b("Не удалось синхронизировать время с биржей.")
            return

    # Тут код работы с двумя экземплярами