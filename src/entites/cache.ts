import { Lock } from "@upstash/lock";
import { Redis } from "@upstash/redis";
import { z } from "zod";

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

export const setCache = async <T>(key: string, value: T) => {
  await redis.set(key, JSON.stringify(value));
};

export const getCache = async <T>(key: string, schema: z.ZodSchema<T>) => {
  const data = await redis.get(key);

  const result = schema.safeParse(data);
  if (!result.success) {
    return null;
  }
  return result.data;
};

export const syncSetCache = async <T>(
  key: string,
  getValue: () => Promise<T |null> ,
  syncKey: string,
  lease: number = 5000
) => {
  const lock = new Lock({
    id: syncKey,
    lease, // Hold the lock for 5 seconds
    redis: Redis.fromEnv(),
  });
  if (await lock.acquire()) {
    const value = await getValue();
    if (!value) {
      await lock.release();
      return;
    }

    await setCache(key, value);
    await lock.release();
  } else {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    await syncSetCache(key, getValue, syncKey, lease);
  }
};

export async function delCache(key: string) {
  await redis.del(key);
}
