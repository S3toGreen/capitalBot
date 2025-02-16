import ccxt.pro as ccxtpro
import asyncio
import time

async def main():
    cex = ccxtpro.okx({
        'apiKey': '2c3d67b2-1170-46f3-a990-dd1deb097ee6',
        'secret': '56D49EEFDC41A44A3FB6BAB6BC19D757',
        'password': 'Seto_898998',
    })
    await cex.load_markets()
    symbols = ['BTC/USDT:USDT']

    async def subTick(symbols: list[str]):
        while True:
            try:
                trades = await cex.watch_trades_for_symbols(symbols)
                process_trades(trades)
            except Exception as e:
                print(f"Unexpected Error: {e}")
                break
    
    try:
        await subTick(symbols)
    finally:
        await cex.close()

    # try:
    #     await subTick(['BTC/USDT:USDT'])
    # finally:
    #     await cex.close()
def process_trades(trades):
        for i in trades:
            if i['amount']<100:
                continue
            color = "\x1b[1;92m" if i['side'] == 'buy' else "\x1b[1;91m"
            print(f"{color}{i['datetime']} {i['side']:>4} {i['amount'] * 0.01:7.3f} at {i['price']} (${i['cost']:.0f})\x1b[m")

if __name__=='__main__':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        pass