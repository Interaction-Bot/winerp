class Payloads:
    '''
    Specifies all the payloads available in the system.
    This is a low level class for :meth:`~PayloadTypes`.
    '''
    success = 0
    verification = 1
    request = 2
    response = 3
    error = 4

class PayloadTypes:

    def __init__(self, type: int) -> None:
        '''
        Specifies the type of message.
        Available Types:

        `success`: Successful authorization.
        `verification`: Verification request.
        `request`: Request for a route.
        `response`: Response to a request.
        `error`: Error response.
        '''
        self._type = type
    
    def __repr__(self) -> str:
        return '<PayloadTypes: {}>'.format(self._type)
    
    @property
    def success(self) -> bool:
        '''
        Returns `True` if the message is a success message.
        '''
        return self._type == Payloads.success

    @property
    def verification(self) -> bool:
        '''
        Returns `True` if the message is a verification message.
        '''
        return self._type == Payloads.verification
    
    @property
    def request(self) -> bool:
        '''
        Returns `True` if the message is a request message.
        '''
        return self._type == Payloads.request
    
    @property
    def response(self) -> bool:
        '''
        Returns `True` if the message is a response message.
        '''
        return self._type == Payloads.response
    
    @property
    def error(self) -> bool:
        '''
        Returns `True` if the message is an error message.
        '''
        return self._type == Payloads.error


class MessagePayload:

    def __init__(self, **kwargs):
        '''Represent IPC payload class.'''
        self.id = kwargs.pop('id', None)
        self.type = kwargs.pop('type', None)
        self.route = kwargs.pop('route', None)
        self.traceback = kwargs.pop('traceback', None)
        self.data = kwargs.pop('data', {})
        self.uuid = kwargs.pop('uuid', None)
        self.destination = kwargs.pop('destination', None)
    

    def from_message(self, msg):
        '''
        Makes a payload from message. This is similar to cloning a message's dict to a new dict with same values.
        '''
        self.id = msg.id
        self.type = msg.type
        self.route = msg.route
        self.data = msg.data
        self.traceback = msg.traceback
        self.uuid = msg.uuid
        self.destination = msg.destination
        return self

    def to_dict(self) -> dict:
        '''
        Returns a `dict` representing the message.
        '''
        return {
            'id': self.id,
            'type': self.type,
            'route': self.route,
            'data': self.data,
            'traceback': self.traceback,
            'uuid': self.uuid,
            'destination': self.destination
        }
