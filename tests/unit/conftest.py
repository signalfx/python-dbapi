# Copyright (C) 2019 SignalFx, Inc. All rights reserved.
from opentracing.ext import tags


class BaseSuite(object):

    @staticmethod
    def assert_base_tags(spans):
        for span in spans:
            assert span.tags[tags.DATABASE_TYPE] == 'sql'
            assert span.tags[tags.SPAN_KIND] == tags.SPAN_KIND_RPC_CLIENT
