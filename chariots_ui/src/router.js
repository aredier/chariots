import Vue from 'vue';
import Router from 'vue-router';
import DataPage from './views/DataPage.vue';

Vue.use(Router);

export default new Router({
  routes: [
    {
      path: '/:series_name',
      name: 'series data',
      // route level code-splitting
      // this generates a separate chunk (about.[hash].js) for this route
      // which is lazy-loaded when the route is visited.
      component: DataPage,
    },
  ],
});
