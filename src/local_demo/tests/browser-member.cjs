// General UI regressions use an ordinary member; quota has its own guest test.
module.exports=async function member(context,base){
 const status=await (await context.request.get(base+'/api/account/status/')).json();
 const response=await context.request.post(base+'/accounts/signup/',{headers:{'X-CSRFToken':status.csrf_token},form:{username:'reg'+require('node:crypto').randomUUID().replaceAll('-',''),nickname:'검증회원',password1:'Regression-user-2468!',password2:'Regression-user-2468!'}});
 if(!response.ok())throw new Error('Browser test member signup failed: '+response.status());
};
