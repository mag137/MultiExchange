#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Анализ спреда между биржами с использованием модели Орнштейна-Уленбека
Биржи: OKX vs Poloniex
Инструмент: ETH/USDT
"""

import numpy as np
import pandas as pd
import ccxt
from scipy import stats
from datetime import datetime
import time

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================

CONFIG = {
    'exchange_a': 'poloniex',
    'exchange_b': 'gate',
    'symbol': 'BONK/USDT',
    'timeframe': '1h',
    'limit': 100,
    'z_entry': 1.5,  # Порог входа по Z-score
    'z_exit': 0.5,  # Порог выхода по Z-score
    'timeout': 30,  # Таймаут запросов в секундах
}


# =============================================================================
# ФУНКЦИИ ПОЛУЧЕНИЯ ДАННЫХ
# =============================================================================

def create_exchange(exchange_name, timeout=30):
    """Создаёт экземпляр биржи с настройками"""
    exchange_class = getattr(ccxt, exchange_name)
    return exchange_class({
        'enableRateLimit': True,
        'timeout': timeout * 1000,
        'options': {
            'defaultType': 'spot',
        }
    })


def fetch_ohlcv_safe(exchange, symbol, timeframe, limit, retries=3):
    """Безопасный запрос OHLCV с повторными попытками"""
    for attempt in range(retries):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            if ohlcv and len(ohlcv) > 0:
                return ohlcv
            print(f"⚠️ Пустой ответ от {exchange.name}, попытка {attempt + 1}/{retries}")
            time.sleep(2)
        except ccxt.NetworkError as e:
            print(f"🌐 Сетевая ошибка ({exchange.name}): {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
        except ccxt.ExchangeError as e:
            print(f"❌ Ошибка биржи ({exchange.name}): {e}")
            raise
        except Exception as e:
            print(f"❌ Неожиданная ошибка ({exchange.name}): {e}")
            if attempt < retries - 1:
                time.sleep(2)
            else:
                raise
    return None


def fetch_spread_data(exchange_a_name, exchange_b_name, symbol,
                      timeframe='1h', limit=100, timeout=30):
    """
    Получает данные о спреде с двух бирж.
    Возвращает pandas Series со спредом (логарифмическим).
    """
    print(f"📡 Подключение к биржам: {exchange_a_name.upper()} & {exchange_b_name.upper()}")

    ex_a = create_exchange(exchange_a_name, timeout)
    ex_b = create_exchange(exchange_b_name, timeout)

    try:
        # Загрузка данных
        print(f"⏳ Загрузка OHLCV ({limit} свечей, {timeframe})...")
        ohlcv_a = fetch_ohlcv_safe(ex_a, symbol, timeframe, limit)
        ohlcv_b = fetch_ohlcv_safe(ex_b, symbol, timeframe, limit)

        if not ohlcv_a or not ohlcv_b:
            print("❌ Не удалось получить данные от одной из бирж")
            return None

        print(f"✅ {exchange_a_name.upper()}: {len(ohlcv_a)} свечей")
        print(f"✅ {exchange_b_name.upper()}: {len(ohlcv_b)} свечей")

        # Преобразование в DataFrame
        df_a = pd.DataFrame(ohlcv_a, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df_b = pd.DataFrame(ohlcv_b, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Синхронизация по timestamp
        df_merged = pd.merge(
            df_a[['timestamp', 'close']],
            df_b[['timestamp', 'close']],
            on='timestamp',
            suffixes=('_a', '_b')
        )

        if len(df_merged) < 20:
            print(f"⚠️ Мало синхронизированных данных: {len(df_merged)}")
            return None

        # Расчёт спреда (логарифмический - лучше для процентных отклонений)
        df_merged['spread'] = np.log(df_merged['close_a']) - np.log(df_merged['close_b'])
        df_merged['spread_abs'] = df_merged['close_a'] - df_merged['close_b']

        # Конвертация timestamp
        df_merged['datetime'] = pd.to_datetime(df_merged['timestamp'], unit='ms')

        print(f"✅ Синхронизировано: {len(df_merged)} свечей")

        return df_merged

    finally:
        # Закрытие сессий
        if hasattr(ex_a, 'session') and ex_a.session:
            ex_a.session.close()
        if hasattr(ex_b, 'session') and ex_b.session:
            ex_b.session.close()


# =============================================================================
# ОЦЕНКА ПАРАМЕТРОВ OU
# =============================================================================

def estimate_ou_params(series, dt=1.0):
    """
    Оценка параметров процесса Орнштейна-Уленбека.

    Параметры:
        series: pandas Series со значениями спреда
        dt: шаг времени в часах (1.0 для часовых данных)

    Возвращает:
        dict с параметрами модели
    """
    series = series.dropna()

    if len(series) < 10:
        raise ValueError(f"Недостаточно данных: {len(series)} < 10")

    # Приращения и лаг
    dX = np.diff(series)
    X_prev = series[:-1].values

    # Линейная регрессия: dX = a + b * X_prev + epsilon
    slope, intercept, r_value, p_value, std_err = stats.linregress(X_prev, dX)

    # Параметры OU
    theta = -slope / dt if abs(slope) > 1e-10 else 0.0
    mu = -intercept / slope if abs(slope) > 1e-10 else float(series.mean())

    # Волатильность
    residuals = dX - (intercept + slope * X_prev)
    sigma = np.std(residuals) / np.sqrt(dt)

    # Half-life
    half_life = np.log(2) / theta if theta > 1e-10 else float('inf')

    # Стационарное стандартное отклонение (для Z-score)
    stationary_std = sigma / np.sqrt(2 * theta) if theta > 1e-10 else float(series.std())

    return {
        'theta': theta,
        'mu': mu,
        'sigma': sigma,
        'half_life_periods': half_life,
        'half_life_hours': half_life * dt,
        'r_squared': r_value ** 2,
        'p_value': p_value,
        'std_err': std_err,
        'stationary_std': stationary_std,
        'n_points': len(series),
    }


# =============================================================================
# ТОРГОВЫЕ СИГНАЛЫ
# =============================================================================

def calculate_zscore(current_value, mu, stationary_std):
    """Расчёт Z-score для текущего значения"""
    if stationary_std > 0:
        return (current_value - mu) / stationary_std
    return 0.0


def get_trading_signal(z_score, z_entry=1.5, z_exit=0.5):
    """Определение торгового сигнала по Z-score"""
    abs_z = abs(z_score)

    if abs_z < z_exit:
        return 'NEUTRAL', '🟢 ЗАКРЫТЬ / НЕ ВХОДИТЬ'
    elif abs_z < z_entry:
        return 'WATCH', '🟡 НАБЛЮДАТЬ'
    else:
        if z_score > 0:
            return 'SHORT_SPREAD', '🔴 SHORT СПРЕД (продаём A, покупаем B)'
        else:
            return 'LONG_SPREAD', '🔴 LONG СПРЕД (покупаем A, продаём B)'


# =============================================================================
# ТЕСТ НА СТАЦИОНАРНОСТЬ
# =============================================================================

def test_stationarity(series):
    """Тест Дики-Фуллера на стационарность"""
    try:
        from statsmodels.tsa.stattools import adfuller
        adf_stat, p_value, crit_vals, *_ = adfuller(series.dropna())

        is_stationary = p_value < 0.05
        confidence = '95%' if p_value < 0.05 else ('90%' if p_value < 0.10 else '❌')

        return {
            'adf_statistic': adf_stat,
            'p_value': p_value,
            'is_stationary': is_stationary,
            'confidence': confidence,
            'critical_values': crit_vals,
        }
    except ImportError:
        return {'error': 'statsmodels не установлен'}
    except Exception as e:
        return {'error': str(e)}


# =============================================================================
# ВИЗУАЛИЗАЦИЯ (опционально)
# =============================================================================

def plot_spread_analysis(df, params, z_score):
    """Построение графика спреда с параметрами OU"""
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        fig, axes = plt.subplots(2, 1, figsize=(14, 10))

        # График 1: Спред + среднее
        ax1 = axes[0]
        ax1.plot(df['datetime'], df['spread'], label='Спред (log)', linewidth=1.5, color='blue')
        ax1.axhline(y=params['mu'], color='red', linestyle='--', linewidth=2, label=f'μ = {params["mu"]:.4f}')
        ax1.axhline(y=params['mu'] + params['stationary_std'], color='orange', linestyle=':', linewidth=1.5, label='+1σ')
        ax1.axhline(y=params['mu'] - params['stationary_std'], color='orange', linestyle=':', linewidth=1.5, label='-1σ')
        ax1.axhline(y=params['mu'] + 1.5 * params['stationary_std'], color='green', linestyle='-.', linewidth=1.5, label='Порог входа (+1.5σ)')
        ax1.axhline(y=params['mu'] - 1.5 * params['stationary_std'], color='green', linestyle='-.', linewidth=1.5, label='Порог входа (-1.5σ)')

        # Текущее значение
        ax1.scatter([df['datetime'].iloc[-1]], [df['spread'].iloc[-1]],
                    color='black', s=100, zorder=5, label=f'Текущее: {df["spread"].iloc[-1]:.4f}')

        ax1.set_xlabel('Время', fontsize=12)
        ax1.set_ylabel('Спред (log)', fontsize=12)
        ax1.set_title(f'Анализ спреда {CONFIG["symbol"]}: {CONFIG["exchange_a"].upper()} vs {CONFIG["exchange_b"].upper()}', fontsize=14)
        ax1.legend(loc='upper right', fontsize=10)
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')

        # График 2: Z-score
        ax2 = axes[1]
        df['z_score'] = (df['spread'] - params['mu']) / params['stationary_std']
        ax2.plot(df['datetime'], df['z_score'], label='Z-score', linewidth=1.5, color='purple')
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=1, alpha=0.5)
        ax2.axhline(y=CONFIG['z_entry'], color='green', linestyle='--', linewidth=2, label=f'Порог входа (+{CONFIG["z_entry"]})')
        ax2.axhline(y=-CONFIG['z_entry'], color='green', linestyle='--', linewidth=2, label=f'Порог входа (-{CONFIG["z_entry"]})')
        ax2.axhline(y=CONFIG['z_exit'], color='gray', linestyle=':', linewidth=1.5, label=f'Порог выхода (+{CONFIG["z_exit"]})')
        ax2.axhline(y=-CONFIG['z_exit'], color='gray', linestyle=':', linewidth=1.5, label=f'Порог выхода (-{CONFIG["z_exit"]})')

        # Текущий Z-score
        ax2.scatter([df['datetime'].iloc[-1]], [z_score],
                    color='black', s=100, zorder=5, label=f'Текущий Z: {z_score:.3f}')

        ax2.set_xlabel('Время', fontsize=12)
        ax2.set_ylabel('Z-score', fontsize=12)
        ax2.set_title('Z-score спреда', fontsize=14)
        ax2.legend(loc='upper right', fontsize=10)
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')

        plt.tight_layout()

        # Сохранение графика
        filename = f'spread_analysis_{CONFIG["exchange_a"]}_{CONFIG["exchange_b"]}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.png'
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"📊 График сохранён: {filename}")

        plt.show()

    except ImportError:
        print("⚠️ matplotlib не установлен: пропущена визуализация")
    except Exception as e:
        print(f"⚠️ Ошибка визуализации: {e}")


# =============================================================================
# ГЛАВНАЯ ФУНКЦИЯ
# =============================================================================

def main():
    """Основная функция анализа"""
    print("=" * 80)
    print("🔍 АНАЛИЗ СПРЕДА С ИСПОЛЬЗОВАНИЕМ МОДЕЛИ ОРНШТЕЙНА-УЛЕНБЕКА")
    print("=" * 80)
    print(f"📅 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"💹 Инструмент: {CONFIG['symbol']}")
    print(f"🏦 Биржи: {CONFIG['exchange_a'].upper()} vs {CONFIG['exchange_b'].upper()}")
    print(f"⏱️ Таймфрейм: {CONFIG['timeframe']}, {CONFIG['limit']} свечей")
    print("=" * 80)

    # 1. Загрузка данных
    print("\n📡 ШАГ 1: ЗАГРУЗКА ДАННЫХ")
    print("-" * 80)
    df = fetch_spread_data(
        CONFIG['exchange_a'],
        CONFIG['exchange_b'],
        CONFIG['symbol'],
        timeframe=CONFIG['timeframe'],
        limit=CONFIG['limit'],
        timeout=CONFIG['timeout']
    )

    if df is None or len(df) < 20:
        print("❌ Не удалось получить достаточное количество данных")
        return

    # 2. Базовая статистика
    print("\n📈 ШАГ 2: БАЗОВАЯ СТАТИСТИКА")
    print("-" * 80)
    print(f"   Диапазон дат:        {df['datetime'].iloc[0]} → {df['datetime'].iloc[-1]}")
    print(f"   Количество точек:    {len(df)}")
    print(f"   Спред (log):")
    print(f"      • Среднее:        {df['spread'].mean():.6f}")
    print(f"      • Медиана:        {df['spread'].median():.6f}")
    print(f"      • Стд. откл.:     {df['spread'].std():.6f}")
    print(f"      • Мин:            {df['spread'].min():.6f}")
    print(f"      • Макс:           {df['spread'].max():.6f}")
    print(f"      • Текущее:        {df['spread'].iloc[-1]:.6f}")
    print(f"   Спред (абсолютный, USDT):")
    print(f"      • Среднее:        {df['spread_abs'].mean():.4f} USDT")
    print(f"      • Текущее:        {df['spread_abs'].iloc[-1]:.4f} USDT")

    # 3. Оценка параметров OU
    print("\n🔧 ШАГ 3: ПАРАМЕТРЫ ОРНШТЕЙНА-УЛЕНБЕКА")
    print("-" * 80)

    dt = 1.0  # 1 час для часовых данных
    params = estimate_ou_params(df['spread'], dt=dt)

    print(f"   θ (theta)           = {params['theta']:.6f}")
    print(f"   μ (mu)              = {params['mu']:.6f}")
    print(f"   σ (sigma)           = {params['sigma']:.6f}")
    print(f"   Half-life           = {params['half_life_hours']:.2f} часов ({params['half_life_periods']:.2f} периодов)")
    print(f"   Стационарное σ      = {params['stationary_std']:.6f}")
    print(f"   R²                  = {params['r_squared']:.4f}")
    print(f"   P-value             = {params['p_value']:.2e}")
    print(f"   Std Error           = {params['std_err']:.6f}")
    print(f"   Количество точек    = {params['n_points']}")

    # Оценка качества модели
    if params['p_value'] < 0.01:
        print(f"   ✅ Коэффициент значим на уровне 99%")
    elif params['p_value'] < 0.05:
        print(f"   ✅ Коэффициент значим на уровне 95%")
    elif params['p_value'] < 0.10:
        print(f"   ⚠️ Коэффициент значим на уровне 90%")
    else:
        print(f"   ❌ Коэффициент не значим (модель не подходит)")

    # 4. Тест на стационарность
    print("\n🧪 ШАГ 4: ТЕСТ НА СТАЦИОНАРНОСТЬ (ADF)")
    print("-" * 80)
    adf_result = test_stationarity(df['spread'])

    if 'error' not in adf_result:
        print(f"   ADF Statistic:      {adf_result['adf_statistic']:.4f}")
        print(f"   P-value:            {adf_result['p_value']:.4f}")
        print(f"   Стационарность:     {'✅ ДА' if adf_result['is_stationary'] else '❌ НЕТ'} ({adf_result['confidence']})")
    else:
        print(f"   ⚠️ {adf_result['error']}")

    # 5. Торговый сигнал
    print("\n🎯 ШАГ 5: ТОРГОВЫЙ СИГНАЛ")
    print("-" * 80)

    current_spread = df['spread'].iloc[-1]
    z_score = calculate_zscore(current_spread, params['mu'], params['stationary_std'])
    signal_code, signal_text = get_trading_signal(z_score, CONFIG['z_entry'], CONFIG['z_exit'])

    print(f"   Текущий спред (log):  {current_spread:.6f}")
    print(f"   Z-score:              {z_score:+.4f}")
    print(f"   Сигнал:               {signal_text}")
    print(f"   Порог входа:          |Z| > {CONFIG['z_entry']}")
    print(f"   Порог выхода:         |Z| < {CONFIG['z_exit']}")

    # 6. Рекомендации
    print("\n💡 ШАГ 6: РЕКОМЕНДАЦИИ ДЛЯ ТОРГОВЛИ")
    print("-" * 80)

    # Расчёт потенциальной прибыли
    expected_move = abs(current_spread - params['mu'])
    expected_profit_usdt = (np.exp(expected_move) - 1) * 100  # Примерно в %

    print(f"   • Ожидаемое время удержания:  ~{params['half_life_hours'] * 2:.1f} часов (2 half-lives)")
    print(f"   • Потенциальное движение:     {expected_move:.4f} ({expected_profit_usdt:.2f}%)")
    print(f"   • Мин. прибыль для комиссий:  > {params['sigma'] * 0.5:.4f} (в log)")
    print(f"   • Переоценка параметров:      каждые 4-6 часов")
    print(f"   • Стоп-лосс по времени:       {params['half_life_hours'] * 4:.1f} часов")

    # Предупреждения
    print(f"\n⚠️  ВАЖНЫЕ ПРЕДУПРЕЖДЕНИЯ:")
    print(f"   • Комиссии бирж не учтены (обычно 0.05-0.1% за сделку)")
    print(f"   • Slippage может составлять 0.1-0.5%")
    print(f"   • При новостях корреляция может нарушаться")
    print(f"   • Всегда тестируйте на исторических данных перед реальной торговлей")

    # 7. Визуализация
    print("\n📊 ШАГ 7: ВИЗУАЛИЗАЦИЯ")
    print("-" * 80)
    plot_spread_analysis(df, params, z_score)

    # 8. Сохранение результатов
    print("\n💾 ШАГ 8: СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
    print("-" * 80)

    results = {
        'timestamp': datetime.now().isoformat(),
        'config': CONFIG,
        'params': params,
        'current_spread': float(current_spread),
        'z_score': float(z_score),
        'signal': signal_code,
        'adf_result': adf_result,
    }

    filename = f'ou_analysis_{CONFIG["exchange_a"]}_{CONFIG["exchange_b"]}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    pd.DataFrame([results]).to_json(filename, orient='records', indent=2, force_ascii=False)
    print(f"   ✅ Результаты сохранены: {filename}")

    # Итог
    print("\n" + "=" * 80)
    print("✅ АНАЛИЗ ЗАВЕРШЁН УСПЕШНО")
    print("=" * 80)

    return results


# =============================================================================
# ЗАПУСК
# =============================================================================

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()