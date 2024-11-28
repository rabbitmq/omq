package utils

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const sampleKubernetesResponse = `
{
  "kind": "Endpoints",
  "apiVersion": "v1",
  "metadata": { },
  "subsets": [
    {
      "addresses": [
        {
          "ip": "10.52.47.2",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "38f11ccb-14dd-41b2-808b-ac30944fa95c"
          }
        },
        {
          "ip": "10.52.48.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "2dfbd120-7e0b-4082-9c50-f62fede0222b"
          }
        },
        {
          "ip": "10.52.49.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "cd4d8793-dee0-482b-a3db-1c1c35967609"
          }
        },
        {
          "ip": "10.52.53.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "f2aeb62f-629e-4882-86af-b3a068132bb5"
          }
        },
        {
          "ip": "10.52.54.5",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "55751cbc-319e-42a6-8203-64aa624505bb"
          }
        },
        {
          "ip": "10.52.55.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "d69cd12e-3d64-451a-ac45-85a6eeca2811"
          }
        },
        {
          "ip": "10.52.56.2",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "b603746f-f75a-4743-8676-852c9a26ae63"
          }
        },
        {
          "ip": "10.52.57.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "7cd52f87-fd50-4468-b8cc-f4f694882adf"
          }
        },
        {
          "ip": "10.52.58.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "ce1cfada-ae4d-499c-9395-ba0c31ff8c7a"
          }
        },
        {
          "ip": "10.52.59.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "36404184-b171-46af-aa68-93486bae361d"
          }
        },
        {
          "ip": "10.52.60.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "2b5e5a02-6b15-49ab-af1d-9d06aa7cc3aa"
          }
        },
        {
          "ip": "10.52.61.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "3b5865e1-1456-4f68-9721-62860306f663"
          }
        },
        {
          "ip": "10.52.62.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "1d1b758c-6efa-495c-8804-4c0010d0f0b9"
          }
        },
        {
          "ip": "10.52.63.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "4bd04dfb-0177-4ad8-bd72-07a4ed54226d"
          }
        },
        {
          "ip": "10.52.64.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "0f247775-c5df-4d2a-a17f-c98bebaea82a"
          }
        },
        {
          "ip": "10.52.65.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "cd8e1194-644a-4028-afe4-24b74d6aea9e"
          }
        },
        {
          "ip": "10.52.66.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "8b5cde92-7446-4010-9aa8-cf6f94f7cadf"
          }
        },
        {
          "ip": "10.52.67.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "aa956a14-d137-4ceb-b804-69d3dad6163c"
          }
        },
        {
          "ip": "10.52.68.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "5ea2b330-eda8-48dc-a948-7958e08421fd"
          }
        },
        {
          "ip": "10.52.69.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "402f3269-f5b6-43bd-9783-5ee2780d4a9c"
          }
        },
        {
          "ip": "10.52.70.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "db7e4f76-3ccb-4c29-a388-3f6fd12d89b5"
          }
        },
        {
          "ip": "10.52.71.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "1368035d-7d74-4c3d-bb5d-44ce54239363"
          }
        },
        {
          "ip": "10.52.72.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "b83fa8cb-0f50-47e9-a0fd-1d2ed6717cd0"
          }
        },
        {
          "ip": "10.52.84.4",
          "nodeName": "foobar",
          "targetRef": {
            "kind": "Pod",
            "namespace": "myspace",
            "name": "barbaz",
            "uid": "fc860b80-3bb8-47c4-8c8c-41f626f44b0b"
          }
        }
      ],
      "ports": [
        {
          "port": 7946,
          "protocol": "TCP"
        }
      ]
    }
  ]
}
`

var _ = FDescribe("parseEndpoints", func() {
	It("returns the correct list of IP addresses from sampleKubernetesResponse", func() {
		ipAddresses, err := parseEndpoints([]byte(sampleKubernetesResponse))
		Expect(err).NotTo(HaveOccurred())

		expectedIPs := []string{
			"10.52.47.2",
			"10.52.48.4",
			"10.52.49.4",
			"10.52.53.4",
			"10.52.54.5",
			"10.52.55.4",
			"10.52.56.2",
			"10.52.57.4",
			"10.52.58.4",
			"10.52.59.4",
			"10.52.60.4",
			"10.52.61.4",
			"10.52.62.4",
			"10.52.63.4",
			"10.52.64.4",
			"10.52.65.4",
			"10.52.66.4",
			"10.52.67.4",
			"10.52.68.4",
			"10.52.69.4",
			"10.52.70.4",
			"10.52.71.4",
			"10.52.72.4",
			"10.52.84.4",
		}
		Expect(ipAddresses).To(Equal(expectedIPs))
	})
})
