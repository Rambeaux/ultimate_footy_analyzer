import inspect

"""
Contant class enabler:
Sourced from:  https://github.com/hmeine/named_constants/blob/master/named_constants.py
Initially Found Here: 
http://stackoverflow.com/questions/2682745/creating-constant-in-python

"""


class _ConstantObject_Meta(type):
    __NamedTypes = {}

    @classmethod
    def NamedValue(cls, typ):
        """Returns a 'NamedTyp' class derived from the given 'typ'.
        The results are cached, i.e. given the same type, the same
        class will be returned in subsequent calls."""
        Const = cls.__NamedTypes.get(typ, None)
        
        if Const is None:
            def __new__(cls, name, value):
                res = typ.__new__(cls, value)
                res._name = name
                res._namespace = None
                return res

            def name(self):
                return self._name
            
            def __repr__(self):
                if self._namespace is None:
                    return self._name
                if self._namespace.__module__ == '__main__':
                    namespace = self._namespace.__name__
                else:
                    namespace = "%s.%s" % (self._namespace.__module__,
                                           self._namespace.__name__)
                return "%s.%s" % (namespace, self._name)

            dct = dict(
                __doc__ = """
Named, typed constant (subclassed from original type, cf. `Constant_Object`
class).  Sole purpose is pretty-printing, i.e. __repr__ returns the
constant's name instead of the original string representations.
The name is also available via a `name()` method.""".lstrip(),
                __new__ = __new__,
                name = name,
                #__str__ = name,
                __repr__ = __repr__)

            if not issubclass(typ, str):
                dct['__slots__'] = ('_name', '_namespace')

            typName = typ.__name__
            name = 'Named' + typName[0].upper() + typName[1:]
            Const = type(name, (typ, ), dct)

            cls.__NamedTypes[typ] = Const

        return Const
    
    def __new__(cls, name, bases, dct):
        constants = {}

        # replace class contents with values wrapped in (typed) Const-class:
        for member in dct:
            value = dct[member]
            if member.startswith('_') or inspect.isfunction(value):
                continue
            Const = cls.NamedValue(type(value))
            
            #print "member{0}: value{1}".format(member, value)
            
            c = Const(member, value)
            constants[member] = c
            dct[member] = c

        dct['__constants__'] = constants
        dct['__reverse__'] = dict((value, value) for key, value in constants.iteritems())
        dct['__sorted__'] = sorted(constants.values())

        result = type.__new__(cls, name, bases, dct)

        # support namespace prefix in __repr__ by connecting the namespace here:
        for c in constants.values():
            c._namespace = result

        return result

    def __len__(self):
        return len(self.__constants__)

    def __iter__(self):
        return iter(self.__sorted__)

    def __setattr__(self, _name, _value):
        raise TypeError('Constant_Object are not supposed to be changed ex post')

    def __contains__(self, x):
        return self.has_key(x) or self.has_value(x)

    def has_key(self, key):
        return self.__constants__.has_key(key)

    def has_value(self, value):
        return self.__reverse__.has_key(value)

    def keys(self):
        return [c.name() for c in self.__sorted__]

    def values(self):
        return self.__sorted__

    def items(self):
        return [(c.name(), c) for c in self.__sorted__]

    def iterkeys(self):
        for c in self.__sorted__:
            yield c.name()

    def itervalues(self):
        return self.__sorted__

    def iteritems(self):
        for c in self.__sorted__:
            yield c.name(), c


class Constant_Object(object):
    """Base class for constant namespaces."""
    __metaclass__ = _ConstantObject_Meta
    __slots__ = ()

    def __new__(cls, x):
        if cls.has_value(x):
            return cls.__reverse__[x]
        if cls.has_key(x):
            return cls.__constants__[x]
        raise ValueError('%s has no key or value %r' % (cls.__name__, x))


# --------------------------------------------------------------------

if __name__ == '__main__':
    import doctest
    doctest.testfile('README.rst')