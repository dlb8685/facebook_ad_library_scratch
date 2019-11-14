import base
import os
import tempfile
import civis
import datetime


if __name__ == "__main__":
    path = "ads_archive"
    access_token = os.environ.get("ACCESS_TOKEN")
    search_page_ids = [124955570892789]
    fields = ["page_id", "page_name", "ad_snapshot_url", "ad_creative_link_title",
              "ad_creative_link_caption", "ad_creative_link_description", 
              "ad_creative_body", "ad_creation_time", 
              "ad_delivery_start_time", "ad_delivery_stop_time",
              "publisher_platforms", "funding_entity", "currency", 
              "impressions", "spend",
              "demographic_distribution", "region_distribution"]
    ad_reached_countries = "US"
    ad_active_status = "ALL"
    ad_type = "POLITICAL_AND_ISSUE_ADS"
    impression_condition = "HAS_IMPRESSIONS_LIFETIME"
    limit = 100

    fb_client = base.BaseClient(access_token)
    fb_client.first_call_no_retry(
        path, access_token=access_token, search_page_ids=search_page_ids, fields=",".join(fields),
        ad_type=ad_type, ad_reached_countries=ad_reached_countries,
        ad_active_status=ad_active_status, impression_condition=impression_condition,
        limit=limit
        )
    
    print("page data: {}".format(datetime.datetime.now()))
    page_data = fb_client.get_data(
        path, access_token=access_token, search_page_ids=search_page_ids, fields=",".join(fields),
        ad_type=ad_type, ad_reached_countries=ad_reached_countries,
        ad_active_status=ad_active_status, impression_condition=impression_condition,
        limit=limit
        )

    with tempfile.NamedTemporaryFile() as fb_ads_file:
        print("gen csv: {}".format(datetime.datetime.now()))
        base.gen_csv(page_data, fields, fb_ads_file.name)
        print("creating job: {}".format(datetime.datetime.now()))
        job = civis.io.csv_to_civis(filename=fb_ads_file.name,
                                      database="redshift-general",
                                      table="scratch.dev_dbryan_fb_ads_library",
                                      headers=True,
                                      max_errors=0,
                                      existing_table_rows="drop")
        print("running job: {}".format(datetime.datetime.now()))
        job.result()
        print("job complete: {}".format(datetime.datetime.now()))
