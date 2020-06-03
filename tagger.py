import csv
from sys import stdin, stdout
from session import Session
import asyncio
from asyncio import ensure_future as spawn
import re


def statevid_col(row):
    pattern = re.compile(r'NY[0-9]{18}')
    try:
        return next(i for i, cell in enumerate(row)
                    if pattern.match(cell) is not None)
    except StopIteration:
        return None


async def main():
    from tqdm import tqdm
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument('--nbids', required=True)
    parser.add_argument('--list', required=True)
    parser.add_argument('--tag', action='append')
    args = parser.parse_args()
    tags = tuple(set(args.tag) | {'sam_was_here'})

    st_to_nb = dict(())
    with open(args.nbids, encoding='utf-8') as istrm:
        rows = csv.reader(istrm)
        head = next(rows)
        nbuid_col = next(i for i, cell in enumerate(head)
                         if 'nationbuilder_id' in cell.lower())
        stvid_col = next(i for i, cell in enumerate(head)
                         if 'state_file_id' in cell.lower())
        for row in rows:
            try:
                vid = int(row[stvid_col][2:])
                uid = int(row[nbuid_col])
                st_to_nb[vid] = uid
            except ValueError:
                continue

    rows = list(csv.reader(stdin))
    steno = csv.writer(stdout, dialect='unix')
    steno.writerow(rows[0])
    stvid_col = None
    while True:
        if len(rows) == 0:
            raise ValueError('no column matching state file ID pattern found')

        stvid_col = statevid_col(rows[0])
        if stvid_col is None:
            rows = rows[1:]
        else:
            break

    uids = []
    for row in rows:
        vid = int(row[stvid_col][2:])
        if vid not in st_to_nb:
            steno.writerow(row)
        else:
            uids.append(st_to_nb[vid])

    async with Session('wiltforcongress') as nb:
        lsid = None
        async for lst in nb.hydrate('/lists'):
            if lst['slug'] == args.list:
                lsid = lst['id']
                break

        if lsid is None:
            author_id = await nb.get('/people/me')['person']['id']
            lst = await nb.post('/lists',
                                {'list': {'name': args.list,
                                          'slug': args.list,
                                          'author_id': author_id}})
            lsid = lst['list_resource']['id']

        # add people to list
        tasks = []
        k = 100000
        while len(uids) > 0:
            task = nb.post(f'/lists/{lsid}/people',
                           {'people_ids': tuple(uids[:k])})
            tasks.append(spawn(task))
            uids = uids[k:]

        for tag in tags:
            tasks.append(spawn(nb.post(f'/lists/{lsid}/tag/{tag}')))

        await asyncio.wait(tasks)


if __name__ == '__main__':
    asyncio.run(main())
