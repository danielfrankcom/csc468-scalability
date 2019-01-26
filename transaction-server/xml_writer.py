import xml.etree.ElementTree as ET
from xml.dom import minidom


"""
VERIFICATION FUNCTION DEFINITIONS
"""

def _isTimestamp(candidate):
    if (isinstance(candidate, int)
            and candidate >= 1514764800000
            and candidate <= 1525132800000):
        return True
    return False

def _isPositiveInt(candidate):
    if (isinstance(candidate, int)
            and candidate > 0):
        return True
    return False

def _isStockSymbol(candidate):
    if (isinstance(candidate, str)
            and len(candidate) > 0
            and len(candidate) <= 3):
        return True
    return False

def _isCommand(candidate):
    valid = [
            "ADD",
            "QUOTE",
            "BUY",
            "COMMIT_BUY",
            "CANCEL_BUY",
            "SELL",
            "COMMIT_SELL",
            "CANCEL_SELL",
            "SET_BUY_AMOUNT",
            "CANCEL_SET_BUY",
            "SET_BUY_TRIGGER",
            "SET_SELL_AMOUNT",
            "SET_SELL_TRIGGER",
            "CANCEL_SET_SELL",
            "DUMPLOG",
            "DISPLAY_SUMMARY"
    ]
    if (isinstance(candidate, str)
            and candidate in valid):
        return True
    return False


"""
CLASS DEFINITIONS
"""

"""
Represents the baseclass for all log events. Field members should not be accessed directly, and
all modifications to attributes should be done through the provided functions. Accessors with 
a prepended '_' symbol should be considered private.
"""
class _LogEvent:

    """
    Initialize the event.

    tag:            a string representing the tag type used in the XML
    supportedTypes: a dictionary of form - "key": (verification_function, is_a_mandatory_field)
    """
    def __init__(self, tag, **supportedTypes):
        self._tag = tag

        self._attributes = dict()
        self._supportedTypes = {
                "timestamp": (lambda x: _isTimestamp(x), True), 
                "server": (lambda x: isinstance(x, str), True),
                "transactionNum": (lambda x: _isPositiveInt(x), True)
        }
        self._supportedTypes.update(supportedTypes)

    """
    Update the current set of attributes with new values or new attributes.

    **args: an unpacked list of key/value pairs to addform `updateAll(key1=value1, key2=value2)'
            or in the form `updateAll(**{key1: value1, key2: value2})' where the '**' indicates
            an unpacked dictionary
    throws: ValueError if one of the provided key/value pairs is invalid
    """
    def updateAll(self, **args):
        validKeys = self._supportedTypes.keys()
        for key, value in args.items():

            if key not in validKeys:
                raise ValueError("Key '{}' is not valid for this event type '{}'.".format(key, self._tag))

            attributeTuple = self._supportedTypes[key]
            validationFunction = attributeTuple[0]
            if not validationFunction(value):
                raise ValueError("Value '{}' is not a valid '{}'.".format(value, key))

            self._attributes[key] = value

    """
    Update or create a specific key/value pair of attributes.

    key:    the key to update/set
    value:  the value to associate with the key
    throws: ValueError if one of the provided key/value pairs is invalid
    """
    def update(self, key, value):
        self.updateAll(**{key: value})

    """
    INTERNAL - don't use

    Get an ET.Element that represents this object.
    throws: ValueError if one of the mandatory attributes is not assigned
    """
    def _getElement(self):
        root = ET.Element(self._tag)

        # Look for all mandatory attributes and store them for processing.
        remaining = [key for key, value in self._supportedTypes.items() if value[1]]
        
        for key, value in self._attributes.items():
            elem = ET.SubElement(root, key)

            if isinstance(value, float):
                # Format to 2 decimal places.
                elem.text = "{0:.2f}".format(value)
            else:
                elem.text = str(value)

            # 'remaining' should be empty when all attributes accessed.
            if key in remaining:
                remaining.remove(key)

        if remaining:
            keyword = "element" if len(remaining) == 1 else "elements"
            raise ValueError("Mandatory {} {} not provided.".format(keyword, remaining))

        return root

"""
From requirements:
    "User commands come from the user command files or from manual entries in the
    students' web forms"
"""
class UserCommand(_LogEvent):

    _supportedTypes = {
            "command": (lambda x: _isCommand(x), True), 
            "username": (lambda x: isinstance(x, str), False),
            "stockSymbol": (lambda x: _isStockSymbol(x), False),
            "filename": (lambda x: isinstance(x, str), False),
            "funds": (lambda x: isinstance(x, float), False)
    }

    def __init__(self, **args):
        _LogEvent.__init__(self, "userCommand", **self._supportedTypes)
        self.updateAll(**args)

"""
From requirements:
    "Every hit to the quote server requires a log entry with the results. The price,
    symbol, username, timestamp and cryptokey are as returned by the quote server"
"""
class QuoteServer(_LogEvent):

    _supportedTypes = {
            "price": (lambda x: isinstance(x, float), True), 
            "username": (lambda x: isinstance(x, str), True),
            "stockSymbol": (lambda x: _isStockSymbol(x), True),
            "quoteServerTime": (lambda x: isinstance(x, int), True),
            "cryptokey": (lambda x: isinstance(x, str), True)
    }

    def __init__(self, **args):
        _LogEvent.__init__(self, "quoteServer", **self._supportedTypes)
        self.updateAll(**args)

"""
From requirements:
    "Any time a user's account is touched, an account message is printed.  
    Appropriate actions are 'add' or 'remove'"
"""
class AccountTransaction(_LogEvent):

    _supportedTypes = {
            "action": (lambda x: isinstance(x, str), True), 
            "username": (lambda x: isinstance(x, str), True),
            "funds": (lambda x: isinstance(x, float), True),
    }

    def __init__(self, **args):
        _LogEvent.__init__(self, "accountTransaction", **self._supportedTypes)
        self.updateAll(**args)

"""
From requirements:
    "System events can be current user commands, interserver communications, 
    or the execution of previously set triggers"
"""
class SystemEvent(_LogEvent):

    _supportedTypes = {
            "command": (lambda x: _isCommand(x), True), 
            "username": (lambda x: isinstance(x, str), False),
            "stockSymbol": (lambda x: _isStockSymbol(x), False),
            "filename": (lambda x: isinstance(x, str), False),
            "funds": (lambda x: isinstance(x, float), False),
    }

    def __init__(self, **args):
        _LogEvent.__init__(self, "systemEvent", **self._supportedTypes)
        self.updateAll(**args)

"""
From requirements:
    "Error messages contain all the information of user commands, in 
    addition to an optional error message"
"""
class ErrorEvent(SystemEvent):

    def __init__(self, **args):
        typeDef = (lambda x: isinstance(x, str), False)
        _LogEvent.__init__(self, "errorEvent", **self._supportedTypes, errorMessage=typeDef)
        self.updateAll(**args)

"""
From requirements:
    "Debugging messages contain all the information of user commands, in 
    addition to an optional debug message"
"""
class DebugEvent(SystemEvent):

    def __init__(self, **args):
        typeDef = (lambda x: isinstance(x, str), False)
        _LogEvent.__init__(self, "debugEvent", **self._supportedTypes, debugMessage=typeDef)
        self.updateAll(**args)

"""
Represents a builder that can construct and write a log file. Derivations of the _LogEvent
class should be created, and appended to the existing set of events stored in the builder
through the use of the #append(event) method.
"""
class LogBuilder:

    """
    Initialize the builder.
    """
    def __init__(self):
        self._root = ET.Element("log")

    """
    Initialize the builder.

    event:  a derivation of the _LogEvent class
    throws: ValueError if one of the mandatory attributes is not assigned
    """
    def append(self, event):
        element = event._getElement()
        self._root.append(element)

    """
    Write the constructed data to a file.

    filePath: the relative or absolute path that the logfile will be created as
    """
    def write(self, filePath):
        rawOutput = ET.tostring(self._root)
        parsed = minidom.parseString(rawOutput)
        pretty = parsed.toprettyxml(indent="   ")

        with open(filePath, "w") as f:
                f.write(pretty)


"""
ENTRY POINT
"""

def main():

    # This code here is not intended to be run in production, and is 
    # meant to demonstrate the basic usage of this mini library.

    builder = LogBuilder()

    event = UserCommand(transactionNum=1, server="name of server")
    event.updateAll(timestamp=1514764800000, command="ADD")
    builder.append(event)

    event = QuoteServer(timestamp=1514764800100, server="name of server")
    attributes = {
            "transactionNum": 2,
            "price": 12.94,
            "stockSymbol": "BER",
            "username": "user",
            "quoteServerTime": 30000012,
            "cryptokey": "sthbaleochb234bsm"
    }
    event.updateAll(**attributes)
    builder.append(event)

    event = UserCommand()
    event.update("transactionNum", 3)
    event.update("server", "name of server")
    event.updateAll(timestamp=1514764800200, command="BUY", username="optional")
    builder.append(event)

    event = ErrorEvent()
    event.updateAll(transactionNum=4, server="name of server", timestamp = 1514764800300)
    event.updateAll(command="BUY", stockSymbol="TED", errorMessage="error here")
    builder.append(event)

    event = SystemEvent()
    event.updateAll(transactionNum=5, server="name of server", timestamp = 1514764800400)
    event.updateAll(command="BUY", stockSymbol="TED")
    builder.append(event)

    event = AccountTransaction()
    event.updateAll(transactionNum=6, server="name of server", timestamp = 1514764800500)
    event.updateAll(action="did something", username="fake", funds=30.00)
    builder.append(event)

    event = DebugEvent()
    event.updateAll(transactionNum=7, funds=40.0, server="name of server", timestamp = 1514764800600)
    event.updateAll(command="BUY", stockSymbol="TED", debugMessage="not required")
    builder.append(event)

    builder.write("./output-log.xml") # XML output.


if __name__ == "__main__":
    main()
