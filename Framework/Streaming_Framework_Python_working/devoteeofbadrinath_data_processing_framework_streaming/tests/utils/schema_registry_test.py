from unittest.mock import patch
from requests.auth import HTTPBasicAuth
from brdj_stream_processing.utils.schema_registry import get_schema_from_registry

@patch('requests.get')
def test_get_schema_from_registry(mock_get):

    mock_response = mock_get.return_value
    mock_response.raise_for_status.return_Value = None
    mock_response.json.return_value = {
        "type" : "record",
        "name": "TestRecord"
    }

    schema_registry_details = {
        "subject": "test_subject",
        "url" : "http://localhost:8081"
    }
    user_id = "dummy_id"
    workload_password = "dummy_password"
    no_proxy = "eks.eu-west-1.amazonaws.com,\
        autoscaling.eu-west-1.amazonaws.com,\
        cloudformation.eu-west-1.amazonaws.com,.boicssp.net,.cloudera.site"
    no_proxy_domains = no_proxy.split(",")
    proxies = {
        "http": None,
        "https": None,
        "no_proxy": ",".join(no_proxy_domains)
    }

    result = get_schema_from_registry(
        schema_registry_details, user_id, workload_password
    )

    mock_get.assert_called_once_with(
        "http://localhost:8081/schemaregistry/schemas/"
        "test_subject/versions/latest/schemaText",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        auth=HTTPBasicAuth(user_id, workload_password),
        proxies=proxies,
        verify=False,
        timeout=100
    )

    assert result == {"type": "record", "name": "TestRecord"}