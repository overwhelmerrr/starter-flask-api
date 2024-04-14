import asyncio
import asyncpg
# import logging
import pandas as pd
from asyncpg.pool import Pool
from dotenv import load_dotenv
import os

load_dotenv('process.env')

DATABASE_URL = os.getenv('DATABASE_URL')
# Игнат лучший

class TCPServer:
    def __init__(self, host, port, database_url,
                 max_size_db_pool=5, max_size_auth_pool=3):
        self.host = host
        self.port = port
        self.database_url = database_url
        self.max_size_db_pool = max_size_db_pool
        self.max_size_auth_pool = max_size_auth_pool
        # self.logger = logging.getLogger('server')
        # self.logger.setLevel(logging.INFO)
        # formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        # file_handler = logging.FileHandler('server.log')
        # file_handler.setFormatter(formatter)
        # self.logger.addHandler(file_handler)
        self.connection_pool: Pool = None
        self.auth_connection_pool: Pool = None
        self.semaphore = asyncio.Semaphore(6)

    async def authenticate(self, reader, writer):
        data = await reader.read(1024)
        auth_info = data.decode().split(';')

        # if len(auth_info) != 3:
        #     writer.write(b'Invalid credentials\n')
        #     self.logger.error(f'Login attempt with data: {data.decode()}')
        #     await writer.drain()
        #     writer.close()
        #     await writer.wait_closed()
        #     return False


        # self.logger.info(f'Login attempt with username: {auth_info[1]}, password: {auth_info[2]}')

        if auth_info[1] in [1, 'TryPerzh']:
            writer.write(b'Authorization;Info;Success')
            await writer.drain()
            print('Игнат прошел аутентификацию')
            return True
        else:
            writer.write(b'Authorization;Error;Invalid')
            await writer.drain()
            print('Назар не прошел аутентификацию')
            return False

    async def calculate(self, reader, writer):
        async with self.connection_pool.acquire() as connection:
            print(f"выделили подключение для {writer.get_extra_info('peername')}")
            result = await connection.fetch('''SELECT *
                                                        FROM (
                                                            SELECT *
                                                            FROM albiondata."BuyOrderDB"
                                                            LIMIT 300
                                                        ) AS buy
                                                        CROSS JOIN (
                                                            SELECT *
                                                            FROM albiondata."SellOrderDB"
                                                            LIMIT 100
                                                        ) AS sell;''')
            df = pd.DataFrame(result)
            df_head = df.head(2)
            response = df_head.to_string(index=False).encode()

            address = writer.get_extra_info('peername')
            # self.logger.info(f'Calculation request from {address}: {response}')

            writer.write(response)
            await writer.drain()

    async def auth(self, login, password):
        pass

    async def update_data(self):
        pass

    async def handle_client(self, reader, writer):
        try:
            async with self.semaphore:

                address = writer.get_extra_info('peername')
                # self.logger.info(f'Connection from {address}')
                print('Назар пришел')

                authenticated = await self.authenticate(reader, writer)

                if not authenticated:
                    return

                # writer.write(b'Success login')
                # await writer.drain()

                while True:
                    data = await reader.read(100)
                    message = data.decode()
                    case_string = message.strip(';')[0]
                    # if not message.strip(';'):
                    #     break
                    if case_string == 'calculate':
                        await self.calculate(reader, writer)
        except:
            print('ss')

    async def run_server(self):
        self.connection_pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=self.max_size_db_pool)
        # self.auth_connection_pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=self.max_size_auth_pool)
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    server = TCPServer('0.0.0.0', 8888, DATABASE_URL, 4)
    asyncio.run(server.run_server())
