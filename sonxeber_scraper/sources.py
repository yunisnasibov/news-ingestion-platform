from __future__ import annotations

from .apa_client import ApaClient
from .axar_client import AxarClient
from .azxeber_client import AzxeberClient
from .azertag_client import AzertagClient
from .azerbaijan_az_client import AzerbaijanAzClient
from .client import SonxeberClient
from .config import Settings
from .ikisahil_client import IkiSahilClient
from .islam_client import IslamClient
from .islamazeri_client import IslamAzeriClient
from .milli_client import MilliClient
from .metbuat_client import MetbuatClient
from .one_news_client import OneNewsClient
from .oxu_client import OxuClient
from .report_client import ReportClient
from .sia_client import SiaClient
from .siyasetinfo_client import SiyasetinfoClient
from .teleqraf_client import TeleqrafClient
from .xeberler_client import XeberlerClient
from .yeniazerbaycan_client import YeniAzerbaycanClient
from .yenixeber_client import YenixeberClient


def build_clients(settings: Settings) -> dict[str, object]:
    clients = {
        ApaClient.source_name: ApaClient(settings),
        AxarClient.source_name: AxarClient(settings),
        AzxeberClient.source_name: AzxeberClient(settings),
        AzerbaijanAzClient.source_name: AzerbaijanAzClient(settings),
        AzertagClient.source_name: AzertagClient(settings),
        IkiSahilClient.source_name: IkiSahilClient(settings),
        IslamClient.source_name: IslamClient(settings),
        IslamAzeriClient.source_name: IslamAzeriClient(settings),
        MilliClient.source_name: MilliClient(settings),
        MetbuatClient.source_name: MetbuatClient(settings),
        OneNewsClient.source_name: OneNewsClient(settings),
        ReportClient.source_name: ReportClient(settings),
        SiaClient.source_name: SiaClient(settings),
        SiyasetinfoClient.source_name: SiyasetinfoClient(settings),
        SonxeberClient.source_name: SonxeberClient(settings),
        TeleqrafClient.source_name: TeleqrafClient(settings),
        XeberlerClient.source_name: XeberlerClient(settings),
        YeniAzerbaycanClient.source_name: YeniAzerbaycanClient(settings),
        OxuClient.source_name: OxuClient(settings),
        YenixeberClient.source_name: YenixeberClient(settings),
    }
    return clients
