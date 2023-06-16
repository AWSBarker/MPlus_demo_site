# directory file to pass extra arguements to server
def process_request(request):
    ''' If present this function is called when the HTTP request arrives. '''
    test = 'this is request_handler'
    return {test : test}