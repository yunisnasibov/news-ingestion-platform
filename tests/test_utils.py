import unittest

from sonxeber_scraper.utils import (
    extract_one_news_article_id,
    extract_one_news_slug,
    extract_apa_article_id,
    extract_apa_category_slug,
    extract_apa_slug,
    extract_axar_article_id,
    extract_axar_category_slug,
    extract_azxeber_category_slug,
    extract_azxeber_slug,
    extract_azerbaijan_az_article_id,
    extract_azertag_article_id,
    extract_ikisahil_article_id,
    extract_ikisahil_slug,
    extract_islam_article_id,
    extract_islam_slug,
    extract_islamazeri_image_article_id,
    extract_islamazeri_slug,
    extract_iqtisadiyyat_article_id,
    extract_iqtisadiyyat_slug,
    extract_milli_article_id,
    extract_milli_category_slug,
    extract_metbuat_article_id,
    extract_metbuat_slug,
    extract_oxu_shortlink_article_id,
    extract_published_date_raw,
    extract_report_shortlink_article_id,
    extract_sia_article_id,
    extract_sia_category_slug,
    extract_siyasetinfo_article_id,
    extract_teleqraf_article_id,
    extract_teleqraf_category_slug,
    extract_xeberler_article_id,
    extract_xeberler_slug,
    extract_yeniazerbaycan_article_id,
    extract_yeniazerbaycan_category_slug,
    extract_yenixeber_article_id,
    extract_yenixeber_slug,
    extract_slug,
    extract_source_article_id,
    fix_utf8_mojibake,
    is_valid_one_news_article_url,
    is_valid_apa_article_url,
    is_valid_axar_article_url,
    is_valid_azxeber_article_url,
    is_valid_azerbaijan_az_article_url,
    is_valid_ikisahil_article_url,
    is_valid_islam_article_url,
    is_valid_islamazeri_article_url,
    is_valid_iqtisadiyyat_article_url,
    is_valid_milli_article_url,
    is_valid_sia_article_url,
    is_valid_siyasetinfo_article_url,
    is_valid_teleqraf_article_url,
    is_valid_xeberler_article_url,
    is_valid_yeniazerbaycan_article_url,
    is_valid_yenixeber_article_url,
    normalize_url,
    parse_apa_datetime,
    parse_axar_datetime,
    parse_azertag_datetime,
    parse_iso_or_dotted_date,
    parse_azerbaijani_date,
    parse_azerbaijani_datetime,
    parse_islamazeri_datetime,
    parse_iqtisadiyyat_datetime,
    parse_one_news_datetime,
    parse_rfc2822_datetime,
    parse_yeniazerbaycan_datetime,
    parse_xeberler_datetime,
)


class UtilsTestCase(unittest.TestCase):
    def test_extract_source_article_id(self) -> None:
        self.assertEqual(
            extract_source_article_id(
                "https://sonxeber.az/391815/bakida-dehsetli-hadise-azyaslilar-atalarinin-avtomobili-ile-iki-qadini-vurdular-biri-oldu"
            ),
            391815,
        )

    def test_extract_slug(self) -> None:
        self.assertEqual(
            extract_slug("https://sonxeber.az/391869/zaur-eliyev-nazir-muavini-teyin-olundu"),
            "zaur-eliyev-nazir-muavini-teyin-olundu",
        )

    def test_extract_one_news_article_id(self) -> None:
        self.assertEqual(
            extract_one_news_article_id(
                "https://1news.az/az/news/20260409120640547-Prezident-Indiyedek-400-den-chox-insan-mina-partlayishlarinda-helak-olub-ve-ya-yaralanib"
            ),
            20260409120640547,
        )

    def test_extract_one_news_slug(self) -> None:
        self.assertEqual(
            extract_one_news_slug(
                "https://1news.az/az/news/20260409120640547-Prezident-Indiyedek-400-den-chox-insan-mina-partlayishlarinda-helak-olub-ve-ya-yaralanib"
            ),
            "Prezident-Indiyedek-400-den-chox-insan-mina-partlayishlarinda-helak-olub-ve-ya-yaralanib",
        )

    def test_extract_published_date_raw(self) -> None:
        self.assertEqual(
            extract_published_date_raw("Sosial Baxılıb: 242 Tarix: 07 aprel 2026"),
            "07 aprel 2026",
        )

    def test_parse_azerbaijani_date(self) -> None:
        self.assertEqual(parse_azerbaijani_date("07 aprel 2026"), "2026-04-07")

    def test_extract_oxu_shortlink_article_id(self) -> None:
        self.assertEqual(
            extract_oxu_shortlink_article_id("https://oxu.az/1515150"),
            1515150,
        )

    def test_extract_metbuat_article_id(self) -> None:
        self.assertEqual(
            extract_metbuat_article_id(
                "https://metbuat.az/news/1550585/kecmis-hakim-yeni-pesesinde-milyonlar-qazanir.html"
            ),
            1550585,
        )

    def test_extract_azertag_article_id(self) -> None:
        self.assertEqual(
            extract_azertag_article_id("https://special.azertag.az/az/xeber/4111987"),
            4111987,
        )

    def test_extract_azerbaijan_az_article_id(self) -> None:
        self.assertEqual(
            extract_azerbaijan_az_article_id("https://azerbaijan.az/news/18906"),
            18906,
        )

    def test_extract_axar_article_id(self) -> None:
        self.assertEqual(
            extract_axar_article_id("https://axar.az/news/planet/1079434.html"),
            1079434,
        )

    def test_extract_apa_article_id(self) -> None:
        self.assertEqual(
            extract_apa_article_id(
                "https://apa.az/hadise/bakida-islediyi-evden-30-min-manatliq-ogurluq-eden-sexs-saxlanilib-954015"
            ),
            954015,
        )

    def test_extract_apa_slug(self) -> None:
        self.assertEqual(
            extract_apa_slug(
                "https://apa.az/hadise/bakida-islediyi-evden-30-min-manatliq-ogurluq-eden-sexs-saxlanilib-954015"
            ),
            "bakida-islediyi-evden-30-min-manatliq-ogurluq-eden-sexs-saxlanilib",
        )

    def test_extract_apa_category_slug(self) -> None:
        self.assertEqual(
            extract_apa_category_slug(
                "https://apa.az/xarici-siyaset/bakida-azerbaycan-ve-qazaxistan-xin-rehberlerinin-gorusu-kecirilir-954014"
            ),
            "xarici-siyaset",
        )

    def test_extract_axar_category_slug(self) -> None:
        self.assertEqual(
            extract_axar_category_slug("https://axar.az/news/planet/1079434.html"),
            "planet",
        )

    def test_extract_metbuat_slug(self) -> None:
        self.assertEqual(
            extract_metbuat_slug(
                "https://metbuat.az/news/1550585/kecmis-hakim-yeni-pesesinde-milyonlar-qazanir.html"
            ),
            "kecmis-hakim-yeni-pesesinde-milyonlar-qazanir",
        )

    def test_extract_iqtisadiyyat_article_id(self) -> None:
        url = "https://iqtisadiyyat.az/az/post/q-rg-z-stan-2100-cu-il-q-d-r-buzlaqlar-n-n-80-ni-itir-bil-r-170995"
        self.assertEqual(extract_iqtisadiyyat_article_id(url), 170995)
        self.assertEqual(
            extract_iqtisadiyyat_slug(url),
            "q-rg-z-stan-2100-cu-il-q-d-r-buzlaqlar-n-n-80-ni-itir-bil-r",
        )
        self.assertTrue(is_valid_iqtisadiyyat_article_url(url))

    def test_extract_milli_article_id(self) -> None:
        self.assertEqual(
            extract_milli_article_id("https://news.milli.az/society/1323670.html"),
            1323670,
        )
        self.assertIsNone(extract_milli_article_id("https://news.milli.az/tag/2606.html"))

    def test_extract_milli_category_slug(self) -> None:
        self.assertEqual(
            extract_milli_category_slug("https://news.milli.az/society/1323670.html"),
            "society",
        )
        self.assertEqual(
            extract_milli_category_slug("https://news.milli.az/tag/2606.html"),
            "",
        )

    def test_extract_report_shortlink_article_id(self) -> None:
        self.assertEqual(
            extract_report_shortlink_article_id("https://report.az/2844552"),
            2844552,
        )
        self.assertEqual(
            extract_report_shortlink_article_id("report.az/2844552"),
            2844552,
        )

    def test_extract_yenixeber_article_id(self) -> None:
        self.assertEqual(
            extract_yenixeber_article_id(
                "https://yenixeber.az/cinsiyyetini-deyisen-qaraderili-aktrisa-olduruldu-149698"
            ),
            149698,
        )

    def test_extract_yenixeber_slug(self) -> None:
        self.assertEqual(
            extract_yenixeber_slug(
                "https://yenixeber.az/cinsiyyetini-deyisen-qaraderili-aktrisa-olduruldu-149698"
            ),
            "cinsiyyetini-deyisen-qaraderili-aktrisa-olduruldu",
        )

    def test_extract_xeberler_article_id(self) -> None:
        self.assertEqual(
            extract_xeberler_article_id(
                "https://xeberler.az/new/details/ayna:-avtobus-marsrutlarina-onlayn-bilet-satisi-2-defe-artib--38665.htm"
            ),
            38665,
        )

    def test_extract_xeberler_slug(self) -> None:
        self.assertEqual(
            extract_xeberler_slug(
                "https://xeberler.az/new/details/ayna:-avtobus-marsrutlarina-onlayn-bilet-satisi-2-defe-artib--38665.htm"
            ),
            "ayna:-avtobus-marsrutlarina-onlayn-bilet-satisi-2-defe-artib",
        )

    def test_extract_siyasetinfo_article_id(self) -> None:
        self.assertEqual(
            extract_siyasetinfo_article_id("https://siyasetinfo.az/8825"),
            8825,
        )

    def test_extract_yeniazerbaycan_article_id(self) -> None:
        self.assertEqual(
            extract_yeniazerbaycan_article_id(
                "https://www.yeniazerbaycan.com/Din_e140104_az.html"
            ),
            140104,
        )

    def test_extract_yeniazerbaycan_category_slug(self) -> None:
        self.assertEqual(
            extract_yeniazerbaycan_category_slug(
                "https://www.yeniazerbaycan.com/Din_e140104_az.html"
            ),
            "Din",
        )

    def test_extract_islam_article_id(self) -> None:
        self.assertEqual(
            extract_islam_article_id(
                "https://islam.az/70144/azerbaycanda-islam-maliyyesi-dovru-sahibkarlar-ve-vetendaslar-ucun-yeni-qapilar-acilir/"
            ),
            70144,
        )

    def test_extract_islam_slug(self) -> None:
        self.assertEqual(
            extract_islam_slug(
                "https://islam.az/70144/azerbaycanda-islam-maliyyesi-dovru-sahibkarlar-ve-vetendaslar-ucun-yeni-qapilar-acilir/"
            ),
            "azerbaycanda-islam-maliyyesi-dovru-sahibkarlar-ve-vetendaslar-ucun-yeni-qapilar-acilir",
        )

    def test_extract_islamazeri_slug(self) -> None:
        self.assertEqual(
            extract_islamazeri_slug("https://www.islamazeri.com/sabahin-hava-proqnozu9426.html"),
            "sabahin-hava-proqnozu9426",
        )

    def test_extract_islamazeri_image_article_id(self) -> None:
        self.assertEqual(
            extract_islamazeri_image_article_id(
                "https://www.islamazeri.com/image/haber/420x280/sabahin-hava-proqnozu9426-8667.jpg"
            ),
            8667,
        )
        self.assertEqual(
            extract_islamazeri_image_article_id(
                "https://www.islamazeri.com/image/haber/sabahin-hava-proqnozu9426-8667.jpg"
            ),
            8667,
        )

    def test_extract_sia_article_id(self) -> None:
        self.assertEqual(
            extract_sia_article_id("https://sia.az/az/news/politics/1329914.html"),
            1329914,
        )

    def test_extract_sia_category_slug(self) -> None:
        self.assertEqual(
            extract_sia_category_slug("https://sia.az/az/news/politics/1329914.html"),
            "politics",
        )

    def test_is_valid_yenixeber_article_url(self) -> None:
        self.assertTrue(
            is_valid_yenixeber_article_url(
                "https://yenixeber.az/cinsiyyetini-deyisen-qaraderili-aktrisa-olduruldu-149698"
            )
        )
        self.assertFalse(is_valid_yenixeber_article_url("https://yenixeber.az/xeberler"))

    def test_is_valid_one_news_article_url(self) -> None:
        self.assertTrue(
            is_valid_one_news_article_url(
                "https://1news.az/az/news/20260409120640547-Prezident-Indiyedek-400-den-chox-insan-mina-partlayishlarinda-helak-olub-ve-ya-yaralanib"
            )
        )
        self.assertFalse(is_valid_one_news_article_url("https://1news.az/az/lenta/"))

    def test_is_valid_yeniazerbaycan_article_url(self) -> None:
        self.assertTrue(
            is_valid_yeniazerbaycan_article_url(
                "https://www.yeniazerbaycan.com/Din_e140104_az.html"
            )
        )
        self.assertFalse(
            is_valid_yeniazerbaycan_article_url(
                "https://www.yeniazerbaycan.com/SonXeber_az.html"
            )
        )

    def test_is_valid_islam_article_url(self) -> None:
        self.assertTrue(
            is_valid_islam_article_url(
                "https://islam.az/70144/azerbaycanda-islam-maliyyesi-dovru-sahibkarlar-ve-vetendaslar-ucun-yeni-qapilar-acilir/"
            )
        )
        self.assertFalse(is_valid_islam_article_url("https://islam.az/cat/xeberler/"))

    def test_is_valid_islamazeri_article_url(self) -> None:
        self.assertTrue(
            is_valid_islamazeri_article_url(
                "https://www.islamazeri.com/sabahin-hava-proqnozu9426.html"
            )
        )
        self.assertFalse(
            is_valid_islamazeri_article_url(
                "https://www.islamazeri.com/x%C9%99b%C9%99rl%C9%99r/"
            )
        )

    def test_is_valid_sia_article_url(self) -> None:
        self.assertTrue(
            is_valid_sia_article_url("https://sia.az/az/news/politics/1329914.html")
        )
        self.assertFalse(is_valid_sia_article_url("https://sia.az/az/latest/"))

    def test_is_valid_xeberler_article_url(self) -> None:
        self.assertTrue(
            is_valid_xeberler_article_url(
                "https://xeberler.az/new/details/ayna:-avtobus-marsrutlarina-onlayn-bilet-satisi-2-defe-artib--38665.htm"
            )
        )
        self.assertFalse(is_valid_xeberler_article_url("https://xeberler.az/new/content/"))

    def test_is_valid_siyasetinfo_article_url(self) -> None:
        self.assertTrue(is_valid_siyasetinfo_article_url("https://siyasetinfo.az/8825"))
        self.assertFalse(is_valid_siyasetinfo_article_url("https://siyasetinfo.az/category/gundem"))

    def test_extract_teleqraf_article_id(self) -> None:
        self.assertEqual(
            extract_teleqraf_article_id("https://teleqraf.az/news/dunya/537688.html"),
            537688,
        )

    def test_extract_teleqraf_category_slug(self) -> None:
        self.assertEqual(
            extract_teleqraf_category_slug("https://teleqraf.az/news/dunya/537688.html"),
            "dunya",
        )

    def test_extract_ikisahil_article_id(self) -> None:
        self.assertEqual(
            extract_ikisahil_article_id(
                "https://ikisahil.az/post/637899-agdaban-qetliamindan-34-il-otur"
            ),
            637899,
        )

    def test_extract_ikisahil_slug(self) -> None:
        self.assertEqual(
            extract_ikisahil_slug(
                "https://ikisahil.az/post/637899-agdaban-qetliamindan-34-il-otur"
            ),
            "agdaban-qetliamindan-34-il-otur",
        )
        self.assertEqual(
            extract_ikisahil_slug("https://ikisahil.az/post/agdaban-qetliamindan-34-il-otur"),
            "agdaban-qetliamindan-34-il-otur",
        )
        self.assertEqual(
            extract_ikisahil_slug(
                "https://ikisahil.az/post/6-sayli-cezachekme-muessisesinde-yangin-bash-verib"
            ),
            "6-sayli-cezachekme-muessisesinde-yangin-bash-verib",
        )

    def test_extract_azxeber_slug(self) -> None:
        self.assertEqual(
            extract_azxeber_slug(
                "https://azxeber.com/az/ilham-eliyev-qazaxistanin-xarici-isler-ve-neqliyyat-nazirlerini-qebul-etdi-foto/siyaset/"
            ),
            "ilham-eliyev-qazaxistanin-xarici-isler-ve-neqliyyat-nazirlerini-qebul-etdi-foto",
        )

    def test_extract_azxeber_category_slug(self) -> None:
        self.assertEqual(
            extract_azxeber_category_slug(
                "https://azxeber.com/az/ilham-eliyev-qazaxistanin-xarici-isler-ve-neqliyyat-nazirlerini-qebul-etdi-foto/siyaset/"
            ),
            "siyaset",
        )

    def test_is_valid_ikisahil_article_url(self) -> None:
        self.assertTrue(
            is_valid_ikisahil_article_url(
                "https://ikisahil.az/post/637899-agdaban-qetliamindan-34-il-otur"
            )
        )
        self.assertTrue(
            is_valid_ikisahil_article_url(
                "https://ikisahil.az/post/agdaban-qetliamindan-34-il-otur"
            )
        )
        self.assertFalse(
            is_valid_ikisahil_article_url("https://ikisahil.az/post/637857-share")
        )
        self.assertFalse(is_valid_ikisahil_article_url("https://ikisahil.az/lent"))

    def test_is_valid_azxeber_article_url(self) -> None:
        self.assertTrue(
            is_valid_azxeber_article_url(
                "https://azxeber.com/az/ilham-eliyev-qazaxistanin-xarici-isler-ve-neqliyyat-nazirlerini-qebul-etdi-foto/siyaset/"
            )
        )
        self.assertFalse(is_valid_azxeber_article_url("https://azxeber.com/az/xeberler/"))

    def test_is_valid_azerbaijan_az_article_url(self) -> None:
        self.assertTrue(is_valid_azerbaijan_az_article_url("https://azerbaijan.az/news/18906"))
        self.assertFalse(
            is_valid_azerbaijan_az_article_url("https://azerbaijan.az/uploads/news/69d51283c0083.jpeg")
        )

    def test_is_valid_axar_article_url(self) -> None:
        self.assertTrue(is_valid_axar_article_url("https://axar.az/news/planet/1079434.html"))
        self.assertFalse(is_valid_axar_article_url("https://axar.az/news/planet/"))

    def test_is_valid_apa_article_url(self) -> None:
        self.assertTrue(
            is_valid_apa_article_url(
                "https://apa.az/hadise/bakida-islediyi-evden-30-min-manatliq-ogurluq-eden-sexs-saxlanilib-954015"
            )
        )
        self.assertFalse(is_valid_apa_article_url("https://apa.az/all-news"))

    def test_is_valid_milli_article_url(self) -> None:
        self.assertTrue(
            is_valid_milli_article_url("https://news.milli.az/society/1323670.html")
        )
        self.assertFalse(is_valid_milli_article_url("https://news.milli.az/tag/2606.html"))

    def test_is_valid_teleqraf_article_url(self) -> None:
        self.assertTrue(
            is_valid_teleqraf_article_url("https://teleqraf.az/news/dunya/537688.html")
        )
        self.assertFalse(is_valid_teleqraf_article_url("https://teleqraf.az/news/dunya/"))

    def test_parse_azerbaijani_datetime(self) -> None:
        self.assertEqual(
            parse_azerbaijani_datetime("8 Aprel 2026", "23:47"),
            "2026-04-08T23:47:00+04:00",
        )

    def test_parse_one_news_datetime(self) -> None:
        self.assertEqual(
            parse_one_news_datetime("2026-04-09T12:09"),
            "2026-04-09T12:09:00+04:00",
        )
        self.assertEqual(
            parse_one_news_datetime("2026-04-09T12:09:00+04:00"),
            "2026-04-09T12:09:00+04:00",
        )

    def test_parse_iqtisadiyyat_datetime(self) -> None:
        self.assertEqual(
            parse_iqtisadiyyat_datetime("2026-04-22T11:01:00.000Z"),
            "2026-04-22T11:01:00+00:00",
        )
        self.assertEqual(
            parse_iqtisadiyyat_datetime("Wed Apr 22 2026 15:01:00 GMT+0400 (Azerbaijan Standard Time)"),
            "2026-04-22T15:01:00+04:00",
        )

    def test_parse_azertag_datetime(self) -> None:
        self.assertEqual(
            parse_azertag_datetime("2026-04-08 05:00:00"),
            "2026-04-08T05:00:00+04:00",
        )

    def test_parse_axar_datetime(self) -> None:
        self.assertEqual(
            parse_axar_datetime("2026.04.08 / 10:18"),
            "2026-04-08T10:18:00+04:00",
        )

    def test_parse_apa_datetime(self) -> None:
        self.assertEqual(
            parse_apa_datetime("08 aprel 2026 12:04 (UTC +04:00)"),
            "2026-04-08T12:04:00+04:00",
        )

    def test_parse_islamazeri_datetime(self) -> None:
        self.assertEqual(
            parse_islamazeri_datetime("4/9/2026 12:59:28 PM1"),
            "2026-04-09T12:59:28+04:00",
        )
        self.assertEqual(
            parse_islamazeri_datetime("Yayınlanma tarixi: 4/8/2026 7:00:03 PM"),
            "2026-04-08T19:00:03+04:00",
        )

    def test_parse_xeberler_datetime(self) -> None:
        self.assertEqual(
            parse_xeberler_datetime("08-04-2026 / 11:00"),
            "2026-04-08T11:00:00+04:00",
        )
        self.assertEqual(parse_xeberler_datetime("08-04-2026"), "2026-04-08")
        self.assertEqual(
            parse_xeberler_datetime("2026-04-08+11:00:21+0400"),
            "2026-04-08T11:00:21+04:00",
        )

    def test_parse_yeniazerbaycan_datetime(self) -> None:
        self.assertEqual(
            parse_yeniazerbaycan_datetime("08.04.2026 [15:37]"),
            "2026-04-08T15:37:00+04:00",
        )
        self.assertEqual(
            parse_yeniazerbaycan_datetime("2026-04-08 12:15:58"),
            "2026-04-08T12:15:58+04:00",
        )

    def test_parse_rfc2822_datetime(self) -> None:
        self.assertEqual(
            parse_rfc2822_datetime("Wed, 08 Apr 2026 03:19:00 +0400"),
            "2026-04-08T03:19:00+04:00",
        )

    def test_parse_iso_or_dotted_date(self) -> None:
        self.assertEqual(parse_iso_or_dotted_date("2026-04-07"), "2026-04-07")
        self.assertEqual(parse_iso_or_dotted_date("07.04.2026"), "2026-04-07")

    def test_normalize_url(self) -> None:
        self.assertEqual(
            normalize_url("https://oxu.az/dunya/abbas-eraqci-pakistana-tesekkur-etdi/?a=1#top"),
            "https://oxu.az/dunya/abbas-eraqci-pakistana-tesekkur-etdi",
        )

    def test_fix_utf8_mojibake(self) -> None:
        self.assertEqual(
            fix_utf8_mojibake("Ä°ran XÄ°N Pakistana tÉÅÉkkÃ¼r edib"),
            "İran XİN Pakistana təşəkkür edib",
        )
        self.assertEqual(fix_utf8_mojibake("Region"), "Region")


if __name__ == "__main__":
    unittest.main()
