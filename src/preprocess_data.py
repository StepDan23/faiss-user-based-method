import json
import os

import pandas as pd
from tqdm import tqdm

from utils import md5_hash


class Transaction:
    def __init__(self, transaction_id, transaction_datetime, **kwargs):
        self.data = {
            **{"tid": transaction_id, "datetime": transaction_datetime, "products": [],},
            **kwargs,
        }

    def add_item(
        self, product_id: str, product_quantity: float, trn_sum_from_iss: float, trn_sum_from_red: float,
    ) -> None:
        p = {
            "product_id": product_id,
            "quantity": product_quantity,
            "s": trn_sum_from_iss,
            "r": "0" if trn_sum_from_red is None or pd.isna(trn_sum_from_red) else trn_sum_from_red,
        }
        self.data["products"].append(p)

    def as_dict(self,):
        return self.data

    def transaction_id(self,):
        return self.data["tid"]


class ClientHistory:
    def __init__(
        self, client_id,
    ):
        self.data = {
            "client_id": client_id,
            "transaction_history": [],
        }

    def add_transaction(
        self, transaction,
    ):
        self.data["transaction_history"].append(transaction)

    def as_dict(self,):
        return self.data

    def client_id(self,):
        return self.data["client_id"]


class RowSplitter:
    def __init__(self, output_path, n_shards=16,):
        self.n_shards = n_shards
        os.makedirs(
            output_path, exist_ok=True,
        )
        self.outs = []
        for i in range(self.n_shards):
            self.outs.append(open(output_path + "{:02d}.jsons".format(i), "w",))
        self._client = None
        self._transaction = None

    def finish(self,):
        self.flush()
        for outs in self.outs:
            outs.close()

    def flush(self,):
        if self._client is not None:
            self._client.add_transaction(self._transaction.as_dict())
            # rows are sharded by cliend_id
            shard_idx = md5_hash(self._client.client_id()) % self.n_shards
            data = self._client.as_dict()
            self.outs[shard_idx].write(json.dumps(data) + "\n")

            self._client = None
            self._transaction = None

    def consume_row(self, row):
        if self._client is not None and self._client.client_id() != row.client_id:
            self.flush()

        if self._client is None:
            self._client = ClientHistory(client_id=row.client_id)

        if self._transaction is not None and self._transaction.transaction_id() != row.transaction_id:
            self._client.add_transaction(self._transaction.as_dict())
            self._transaction = None

        if self._transaction is None:
            self._transaction = Transaction(
                transaction_id=row.transaction_id,
                transaction_datetime=row.transaction_datetime,
                rpr=row.regular_points_received,
                epr=row.express_points_received,
                rps=row.regular_points_spent,
                eps=row.express_points_spent,
                sum=row.purchase_sum,
                store_id=row.store_id,
            )

        self._transaction.add_item(
            product_id=row.product_id,
            product_quantity=row.product_quantity,
            trn_sum_from_iss=row.trn_sum_from_iss,
            trn_sum_from_red=row.trn_sum_from_red,
        )


def split_data_to_chunks(input_path, output_dir, n_shards):
    already_split = True
    for path in [output_dir + "{:02d}.jsons".format(i) for i in range(n_shards)]:
        already_split &= os.path.exists(path)
    if not already_split:
        splitter = RowSplitter(output_path=output_dir, n_shards=n_shards, )
        print("split_data_to_chunks: {} -> {}".format(input_path, output_dir,))
        for df in tqdm(pd.read_csv(input_path, chunksize=500000, )):
            for row in df.itertuples():
                splitter.consume_row(row)
        splitter.finish()
    else:
        print('Data is already split to chunks')
