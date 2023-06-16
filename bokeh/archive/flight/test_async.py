import asyncio

async def count(x):
    print("One")
    await asyncio.sleep(x)
    print("Two")
    return x+1

async def main():
    out = await count(1)
    #await asyncio.gather(count(), count(), count())
    print(f'after await {out}')

if __name__ == "__main__":
    import time
    s = time.perf_counter()
    asyncio.run(main())
    elapsed = time.perf_counter() - s
    print(f"{__file__} executed in {elapsed:0.2f} seconds.")