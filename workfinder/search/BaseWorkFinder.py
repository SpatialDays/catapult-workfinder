from abc import abstractmethod
import pandas as pd


class NoWorkException(Exception):
    pass


class BaseWorkFinder:
    def __init__(self):
        pass

    def find_new_work(self):
        possible = self.find_work_list()
        done = self.find_already_done_list()

        if not possible:
            raise NoWorkException

        df_possible = _to_pandas(possible)
        df_done = pd.DataFrame({'id': []})
        if done:
            df_done = _to_pandas(done)

        return _diff_work_lists(df_possible, df_done)

    @abstractmethod
    def find_work_list(self):
        """
        find_work_list returns a list of potential work packages.
        it returns a list of dictionaries with identifier and url fields.
        """
        pass

    @abstractmethod
    def find_already_done_list(self):
        """
        find_already_done_list returns a list of work already done. This should be a list of identifiers in the same
        format as the find_work_list. This will be matched to
        """
        pass


def _diff_work_lists(possible, done):
    df_result = pd.DataFrame({'id': [], 'url': []})
    for index, r in possible.iterrows():
        if not (done["id"] == r['id'])[0]:
            df_result = df_result.append({'id': r['id'], 'url': r['url']}, ignore_index=True)
    return df_result


def _to_pandas(input):
    """
    _to_pandas will make sure the input array is a pandas dataframe. if it is not something that can be converted it
    will raise an error.
    """

    return input
