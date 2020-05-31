from urllib.parse import urlencode, parse_qs
import aiohttp
import asyncio
from os import getenv
from time import time
from math import ceil


class Session(object):
    def __init__(self, slug, access_token=getenv('NB_TOKEN'), concurrency=128):
        self.slug = slug
        self.token = access_token
        self.unthrottled = asyncio.Event()
        self.unthrottled.set()
        self.concurrency = concurrency
        self.sem = asyncio.BoundedSemaphore(concurrency)
        self.in_flight = set(())

    async def __aenter__(self):
        self.http = aiohttp.ClientSession(raise_for_status=True)
        return self

    async def __aexit__(self, exn_type, exn, trace):
        await asyncio.gather(self.http.close(), *self.in_flight)

    async def backoff(self, t):
        if self.unthrottled.is_set():
            self.unthrottled.clear()
            await asyncio.sleep(t)
            self.unthrottled.set()
        else:
            await self.unthrottled.wait()

    async def do(self, path, method='GET', payload=None, **kwargs):
        try:
            self.in_flight |= {asyncio.current_task()}
            async with self.sem:
                await self.unthrottled.wait()
                uri = (f'https://{self.slug}.nationbuilder.com/api/v1'
                       f'/{path}?access_token={self.token}&')
                uri += urlencode(kwargs)
                headers = {'Accept': 'application/json'}
                async with self.http.request(method, uri, json=payload,
                                             headers=headers) as rsp:
                    requests_remaining, backoff = (
                            int(rsp.headers['X-Ratelimit-Remaining']),
                            float(rsp.headers['X-Ratelimit-Reset']) - time())
                    if requests_remaining <= self.concurrency:
                        await self.backoff(backoff)
                    return await rsp.json()
        finally:
            self.in_flight -= {asyncio.current_task()}

    async def get(self, path, payload=None, **kwargs):
        return await self.do(self, path, 'GET', payload=payload, **kwargs)

    async def put(self, path, payload=None, **kwargs):
        return await self.do(self, path, 'PUT', payload=payload, **kwargs)

    async def create(self, path, payload=None, **kwargs):
        return await self.do(self, path, 'POST', payload=payload, **kwargs)

    async def delete(self, path, payload=None, **kwargs):
        return await self.do(self, path, 'DELETE', payload=payload, **kwargs)

    async def hydrate(self, path, limit=100, nonce=None, token=None, **kwargs):
        kwargs = dict(())
        if nonce is not None and token is not None:
            kwargs['__nonce'] = nonce
            kwargs['__token'] = token

        payload = await self.do(path, limit=limit, **kwargs)
        for x in payload['results']:
            yield x
        if 'next' not in payload or payload['next'] is not None:
            q = payload['next'].index('?') + 1
            args = parse_qs(payload['next'][q:])
            nonce = args['__nonce'][0]
            token = args['__token'][0]
            async for x in self.hydrate(path, limit, nonce, token):
                yield x


async def main():
    # stress-test rate limiting
    from tqdm import tqdm
    expected_ratelimit = 1000
    bar = tqdm(total=expected_ratelimit*2)

    # not a real task, just spam
    async def dummy(nb, bar):
        await nb.do('/people/me')
        bar.update(1)

    async with Session('wiltforcongress') as nb:
        await asyncio.gather(*[asyncio.ensure_future(dummy(nb, bar))
                               for i in range(expected_ratelimit*2)])


if __name__ == '__main__':
    asyncio.run(main())
