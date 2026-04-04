class HelloSolution:

    # friend_name = unicode string
    def hello(self, friend_name: str):
        """
        Function that returns a string with the message 'Hello, {friend_name}!'
        """

        if type(friend_name) != str:
            return "Hello, World!"
        return f"Hello, {friend_name}!"


