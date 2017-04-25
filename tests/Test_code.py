
import unitest

class TestMaxDtWS():
    def setup(self):
        self.maxdtws = xxx

## test if
## test if max_date = today()-1 in web_sessions 
    def test_today(self):
    	self.assertEqual(self.maxdtws, today)



class TestMaxDtWP():
    def setup(self):
        self.maxdtws = xxx

## test if max_date = today()-1 in web_phones





#assertEqual(first, second, msg=None)
#assertNotEqual(first, second, msg=None)
#assertTrue(expr, msg=None)
#assertFalse(expr, msg=None)
#assertIn(first, second)
#assertRaises(exc, fun, msg=None, *args, **kwargs)



#decorator is used for multiple testing result



class TestDBConnection():
	def setup(self):
		self.db = xxx
    param_names ="dbconnect, dbname, result"
    params = [ ( , fm01, ),
		       ( , fcn1, )
             ]
	@pytest.mark.parametrize(param_names, params)
	def test











if __name__ == '__main__':
	unitest.main()
