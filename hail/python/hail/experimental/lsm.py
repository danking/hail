from hail.utils.java import Env


def region():
    return Env.hc()._jhc.backend().region()


class LSM:
    def __init__(self,
                 path,
                 key_type,
                 key_ptype,
                 value_type,
                 value_ptype,
                 key_codec='{"name":"BlockingBufferSpec","blockSize":65536,"child":{"name":"StreamBlockBufferSpec"}}',
                 value_codec='{"name":"BlockingBufferSpec","blockSize":65536,"child":{"name":"StreamBlockBufferSpec"}}'):
        self.region = region()
        self.key_type = key_type
        self.key_ptype = key_ptype
        self.value_type = value_type
        self.value_ptype = value_ptype
        self.lsm = Env.hc()._jhc.backend().lsm(path, key_ptype, key_codec, value_ptype, value_codec, self.region)

    def _key_from_java(self, koff):
        return self.key_type._from_json(
            Env.hc()._jhc.backend().regionValueToJSON(
                self.key_ptype, koff))

    def _value_from_java(self, voff):
        return self.value_type._from_json(
            Env.hc()._jhc.backend().regionValueToJSON(
                self.value_ptype, voff))

    def _entry_from_java(self, entry):
        key = self._key_from_java(entry.getKey())
        value = self._value_from_java(entry.getValue())
        return (key, value)

    def _key_to_java(self, kir):
        return Env.hc()._jhc.backend().toRegionValue(self.region, Env.backend()._to_java_ir(kir._ir), self.key_ptype)

    def _value_to_java(self, vir):
        return Env.hc()._jhc.backend().toRegionValue(self.region, Env.backend()._to_java_ir(vir._ir), self.value_ptype)

    def put(self, kir, vir):
        koff = self._key_to_java(kir)
        voff = self._value_to_java(vir)
        self.lsm.store().put(koff, voff)

    def get(self, kir):
        koff = self._key_to_java(kir)
        voff = self.lsm.store().get(koff)
        if voff is None:
            return None
        return self._value_from_java(voff)

    def lower(self, kir):
        koff = self._key_to_java(kir)
        return self._entry_from_java(self.lsm.store().lower(koff))

    def floor(self, kir):
        koff = self._key_to_java(kir)
        return self._entry_from_java(self.lsm.store().floor(koff))

    def ceil(self, kir):
        koff = self._key_to_java(kir)
        return self._entry_from_java(self.lsm.store().ceil(koff))

    def higher(self, kir):
        koff = self._key_to_java(kir)
        return self._entry_from_java(self.lsm.store().higher(koff))

    def first(self):
        return self._entry_from_java(self.lsm.store().first())

    def last(self):
        return self._entry_from_java(self.lsm.store().last())

    def __iter__(self):
        it = self.lsm.store().iterator()
        while it.hasNext():
            yield self._entry_from_java(it.next())

    def close(self):
        self.region.close()
